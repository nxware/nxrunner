"""
Netzwerk-Konfiguration ueber NetworkManager (nmcli)

Umgesetzt (jeweils als Signatur/Job und in der UI):
  - general:    Status, Hostname, Berechtigungen, Netzwerk an/aus, Konnektivitaet pruefen
  - radio:      WLAN / WWAN ein- und ausschalten
  - device:     Status, Details, verbinden/trennen, reapply, managed/autoconnect, loeschen (Software-Geraete), LLDP
  - device wifi: Liste, Scan, verbinden (auch versteckte Netze), Hotspot, WLAN-Passwort anzeigen
  - connection: Liste, Details, aktivieren/deaktivieren, anlegen, aendern, klonen, loeschen, neu laden,
                VPN-Import (OpenVPN, WireGuard, ...) und Export

    Config (runners):
        network_manager: nxrunner.network.networkmanager:NetworkManager
        network_manager: nxrunner.network.networkmanager:NetworkManager(sudo=True)

    Job:
        {"type": "network_manager", "op": "con_up", "uuid": "..."}
        {"type": "network_manager", "op": "con_modify", "uuid": "...", "key": "ipv4.dns", "value": "1.1.1.1"}

Aendernde Aktionen brauchen Rechte (polkit oder root). Mit sudo=True wird "sudo -n nmcli" ausgefuehrt.
"""
import html
import json
import shlex
import subprocess

from nwebclient import runner as r
from nwebclient import base
from nwebclient import web as w
from nwebclient import dev as d


class NmResult:
    """ Ergebnis eines nmcli-Aufrufs """

    def __init__(self, ok, out='', err='', cmd=''):
        self.ok = ok
        self.out = out
        self.err = err
        self.cmd = cmd

    def message(self):
        return (self.err or self.out).strip()


def split_terse(line, maxsplit=-1):
    """ Zerlegt eine Zeile der nmcli-Ausgabe im terse-Modus (-t) an nicht maskierten ':' """
    parts, cur, esc = [], '', False
    for ch in line:
        if esc:
            cur += ch
            esc = False
        elif ch == '\\':
            esc = True
        elif ch == ':' and (maxsplit < 0 or len(parts) < maxsplit):
            parts.append(cur)
            cur = ''
        else:
            cur += ch
    parts.append(cur)
    return parts


def yes_no(value):
    """ Wert aus UI/Job (bool, 'true', 'on', 'yes', ...) als 'yes'/'no' """
    if isinstance(value, bool):
        return 'yes' if value else 'no'
    return 'yes' if str(value).strip().lower() in ('1', 'true', 'yes', 'on', 'ja') else 'no'


def esc(value):
    return html.escape('' if value is None else str(value), quote=True)


class NetworkManager(r.BaseJobExecutor):
    """
    Konfiguriert das Netzwerk ueber nmcli (NetworkManager): Geraete, Verbindungsprofile, WLAN, Hotspot, VPN-Import.
    """

    # Argumente, deren nachfolgender Wert in Logs/UI maskiert wird
    SECRET_ARGS = ('password', 'wifi-sec.psk', '802-11-wireless-security.psk', '802-1x.password',
                   'wireguard.private-key', 'vpn.secrets', 'gsm.password', 'pppoe.password')

    # Typ -> (Titel, Beispiel fuer weitere Eigenschaften)
    CON_TYPES = {
        'ethernet': ("Ethernet", "802-3-ethernet.mtu 1500   (als Bridge-Port: master br0 slave-type bridge)"),
        'wifi': ("WLAN-Profil", "ssid MeinNetz wifi-sec.key-mgmt wpa-psk wifi-sec.psk geheim"),
        'bridge': ("Bridge", "bridge.stp no"),
        'bond': ("Bond", "bond.options mode=active-backup,miimon=100"),
        'vlan': ("VLAN", "vlan.parent eth0 vlan.id 10"),
        'macvlan': ("MACVLAN", "macvlan.parent eth0 macvlan.mode bridge"),
        'vxlan': ("VXLAN", "vxlan.id 42 vxlan.remote 192.0.2.1"),
        'ip-tunnel': ("IP-Tunnel (GRE/IPIP)", "ip-tunnel.mode gre ip-tunnel.remote 192.0.2.1"),
        'wireguard': ("WireGuard", "wireguard.listen-port 51820   (Konfiguration besser per Import)"),
        'gsm': ("Mobilfunk (GSM/LTE)", "gsm.apn internet"),
        'pppoe': ("PPPoE (DSL)", "pppoe.username user pppoe.password geheim"),
        'dummy': ("Dummy", ""),
        'loopback': ("Loopback", ""),
    }

    IPV4_METHODS = {'': "(unverändert)", 'auto': "auto (DHCP)", 'manual': "manuell (statisch)",
                    'link-local': "link-local", 'shared': "shared (Internet teilen)", 'disabled': "deaktiviert"}
    IPV6_METHODS = ['auto', 'dhcp', 'manual', 'link-local', 'shared', 'ignore', 'disabled']

    # Eigenschaft -> (Titel, Tooltip, Optionen oder None); angezeigt werden nur vorhandene Eigenschaften
    EDIT_PROPS = {
        'connection.id': ("Name", "Name des Verbindungsprofils", None),
        'connection.autoconnect': ("Autoconnect", "Automatisch aktivieren, sobald das Gerät verfügbar ist",
                                   ['yes', 'no']),
        'connection.autoconnect-priority': ("Priorität", "Höhere Werte werden beim Autoconnect bevorzugt", None),
        'connection.interface-name': ("Interface", "Profil an ein Interface binden, leer = beliebig", None),
        'connection.zone': ("Firewall-Zone", "firewalld-Zone, z.B. home, public, trusted", None),
        'connection.metered': ("Getaktet", "Verbindung als getaktet (Volumentarif) markieren",
                               ['unknown', 'yes', 'no']),
        '802-11-wireless.ssid': ("SSID", "Name des WLAN", None),
        '802-11-wireless.hidden': ("Verstecktes WLAN", "SSID wird nicht ausgestrahlt", ['no', 'yes']),
        '802-11-wireless-security.psk': ("WLAN-Passwort", "Leer lassen = unverändert", 'secret'),
        '802-3-ethernet.mtu': ("MTU", "Maximale Paketgröße, auto = Standard", None),
        '802-3-ethernet.cloned-mac-address': ("MAC-Adresse", "Geklonte MAC: Adresse, preserve, permanent, random",
                                              None),
        'ipv4.method': ("IPv4-Methode", "auto = DHCP, manual = statische Adresse, shared = Internet teilen (NAT)",
                        list(IPV4_METHODS)[1:]),
        'ipv4.addresses': ("IPv4-Adressen", "Statische Adressen mit Präfix, z.B. 192.168.1.10/24 (mehrere mit Komma)",
                           None),
        'ipv4.gateway': ("IPv4-Gateway", "Standard-Gateway, z.B. 192.168.1.1", None),
        'ipv4.dns': ("IPv4-DNS", "DNS-Server, z.B. 1.1.1.1,9.9.9.9", None),
        'ipv4.dns-search': ("DNS-Suchdomänen", "z.B. fritz.box,example.org", None),
        'ipv4.ignore-auto-dns': ("DHCP-DNS ignorieren", "Nur die eingetragenen DNS-Server verwenden", ['no', 'yes']),
        'ipv4.route-metric': ("Routen-Metrik", "Kleinere Werte werden bevorzugt, -1 = Standard", None),
        'ipv4.never-default': ("Nie Standardroute", "Über diese Verbindung keine Standardroute setzen", ['no', 'yes']),
        'ipv6.method': ("IPv6-Methode", "auto = SLAAC/DHCPv6, manual = statisch, disabled = aus", IPV6_METHODS),
        'ipv6.addresses': ("IPv6-Adressen", "z.B. fd00::10/64", None),
        'ipv6.gateway': ("IPv6-Gateway", "z.B. fd00::1", None),
        'ipv6.dns': ("IPv6-DNS", "z.B. 2606:4700:4700::1111", None),
    }

    VPN_IMPORT_TYPES = ['openvpn', 'wireguard', 'vpnc', 'openconnect', 'l2tp', 'pptp', 'strongswan']

    def __init__(self, type='network_manager', sudo=False, nmcli='nmcli', timeout=30):
        super().__init__(type)
        self.sudo = sudo
        self.nmcli_bin = nmcli
        self.timeout = int(timeout)
        self.last_cmd = ''
        self.last_output = ''
        self.define_sigs_general()
        self.define_sigs_devices()
        self.define_sigs_wifi()
        self.define_sigs_connections()

    # ------------------------------------------------------------------
    # Signaturen
    # ------------------------------------------------------------------

    def sig(self, op, description, *params):
        return self.define_sig(d.PStr('op', op), *params).set_description(description)

    def define_sigs_general(self):
        self.sig('status', "Status des NetworkManagers (nmcli general status)").r(success='bool', status='dict')
        self.sig('set_hostname', "Hostname setzen", d.PStr('hostname', '')).r(success='bool', message='str')
        self.sig('networking', "Netzwerk komplett ein-/ausschalten", d.PStr('state', 'on')).r(
            success='bool', message='str')
        self.sig('connectivity', "Internet-Konnektivität prüfen").r(success='bool', connectivity='str')
        self.sig('radio', "Funk ein-/ausschalten (kind: wifi, wwan, all)", d.PStr('kind', 'wifi'),
                 d.PStr('state', 'on')).r(success='bool', message='str')
        self.sig('permissions', "Berechtigungen des Aufrufers").r(success='bool', permissions='dict')

    def define_sigs_devices(self):
        self.sig('devices', "Netzwerkgeräte").r(success='bool', devices='list')
        self.sig('device_show', "Details eines Geräts", d.PStr('device', 'eth0')).r(success='bool', props='dict')
        for op, desc in (('device_connect', "Gerät verbinden"), ('device_disconnect', "Gerät trennen"),
                         ('device_reapply', "Geänderte Einstellungen ohne Trennen übernehmen"),
                         ('device_delete', "Software-Gerät löschen (Bridge, VLAN, ...)")):
            self.sig(op, desc, d.PStr('device', 'eth0')).r(success='bool', message='str')
        self.sig('device_set', "Gerät managed/autoconnect setzen", d.PStr('device', 'eth0'),
                 d.PStr('key', 'autoconnect'), d.PStr('value', 'yes')).r(success='bool', message='str')

    def define_sigs_wifi(self):
        self.sig('wifi_list', "Sichtbare WLAN-Netze", d.PStr('rescan', 'no')).r(success='bool', networks='list')
        self.sig('wifi_rescan', "WLAN-Netze neu suchen").r(success='bool', message='str')
        self.sig('wifi_connect', "Mit WLAN verbinden", d.PStr('ssid', ''), d.PStr('password', ''),
                 d.PStr('ifname', ''), d.PStr('hidden', 'no')).r(success='bool', message='str')
        self.sig('wifi_hotspot', "WLAN-Hotspot starten", d.PStr('ssid', 'nxhotspot'), d.PStr('password', ''),
                 d.PStr('ifname', ''), d.PStr('band', '')).r(success='bool', message='str')
        self.sig('wifi_password', "Passwort des aktiven WLAN anzeigen").r(success='bool', props='dict')

    def define_sigs_connections(self):
        self.sig('connections', "Verbindungsprofile").r(success='bool', connections='list')
        self.sig('con_show', "Eigenschaften eines Profils", d.PStr('uuid', '')).r(success='bool', props='dict')
        for op, desc in (('con_up', "Profil aktivieren"), ('con_down', "Profil deaktivieren"),
                         ('con_delete', "Profil löschen"), ('con_export', "VPN-Profil exportieren")):
            self.sig(op, desc, d.PStr('uuid', '')).r(success='bool', message='str')
        self.sig('con_modify', "Eigenschaft ändern (key mit +/- fügt hinzu/entfernt)", d.PStr('uuid', ''),
                 d.PStr('key', 'ipv4.dns'), d.PStr('value', '')).r(success='bool', message='str')
        self.sig('con_clone', "Profil klonen", d.PStr('uuid', ''), d.PStr('name', '')).r(success='bool', message='str')
        self.sig('con_add', "Profil anlegen", d.PStr('con_type', 'ethernet'), d.PStr('name', ''),
                 d.PStr('ifname', ''), d.PStr('extra', '')).r(success='bool', message='str')
        self.sig('con_import', "VPN-Konfiguration importieren", d.PStr('vpn_type', 'wireguard'),
                 d.PStr('file', '')).r(success='bool', message='str')
        self.sig('con_reload', "Profile von der Festplatte neu laden").r(success='bool', message='str')

    # ------------------------------------------------------------------
    # nmcli
    # ------------------------------------------------------------------

    def masked(self, args):
        """ Kommandozeile fuer Anzeige/Log, Geheimnisse werden durch *** ersetzt """
        res = []
        for i, a in enumerate(args):
            res.append('***' if i > 0 and args[i - 1] in self.SECRET_ARGS else a)
        return ' '.join(shlex.quote(a) for a in res)

    def nmcli(self, *args, timeout=None) -> NmResult:
        """ Fuehrt nmcli aus, z.B. self.nmcli('connection', 'up', 'uuid', uuid) """
        args = [str(a) for a in args]
        cmd = (['sudo', '-n'] if self.sudo else []) + [self.nmcli_bin] + args
        shown = self.masked(cmd)
        try:
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                                 timeout=timeout or self.timeout)
            result = NmResult(res.returncode == 0, res.stdout, res.stderr, shown)
        except FileNotFoundError:
            result = NmResult(False, err=f"{cmd[0]} nicht gefunden (NetworkManager installiert?)", cmd=shown)
        except subprocess.TimeoutExpired:
            result = NmResult(False, err=f"Zeitüberschreitung nach {timeout or self.timeout} s", cmd=shown)
        return result

    def run(self, *args, timeout=None) -> NmResult:
        """ Wie nmcli(), merkt sich Kommando und Ausgabe fuer die UI (fuer aendernde Aktionen) """
        res = self.nmcli(*args, timeout=timeout)
        self.last_cmd = res.cmd
        self.last_output = (res.out + res.err).strip()
        self.info(f"{res.cmd}: {'ok' if res.ok else 'Fehler'}")
        return res

    def query(self, fields, *args):
        """ Tabellarische Ausgabe als Liste von dicts, fields z.B. ['DEVICE', 'STATE'] """
        res = self.nmcli('-t', '-f', ','.join(fields), *args)
        if not res.ok:
            raise RuntimeError(res.message())
        return [dict(zip(fields, split_terse(line))) for line in res.out.splitlines() if line.strip()]

    def show(self, *args):
        """ Schluessel/Wert-Ausgabe (show, multiline) als dict """
        res = self.nmcli('-t', *args)
        if not res.ok:
            raise RuntimeError(res.message())
        props = {}
        for line in res.out.splitlines():
            if ':' in line:
                key, value = split_terse(line, 1)
                props[key] = value
        return props

    def result(self, res: NmResult, **kwargs):
        """ Job-Ergebnis aus einem nmcli-Aufruf """
        if res.ok:
            return self.success(res.message() or 'ok', output=res.out, ui_reload=True, **kwargs)
        return self.fail(res.message() or 'nmcli Fehler', output=res.out + res.err, **kwargs)

    # ------------------------------------------------------------------
    # Daten
    # ------------------------------------------------------------------

    def get_status(self):
        return self.show('-m', 'multiline', 'general', 'status')

    def get_hostname(self):
        res = self.nmcli('general', 'hostname')
        return res.out.strip() if res.ok else ''

    def get_permissions(self):
        return self.query(['PERMISSION', 'VALUE'], 'general', 'permissions')

    def get_devices(self):
        return self.query(['DEVICE', 'TYPE', 'STATE', 'CONNECTION', 'CON-UUID'], 'device', 'status')

    def get_connections(self):
        return self.query(['NAME', 'UUID', 'TYPE', 'DEVICE', 'ACTIVE', 'STATE', 'AUTOCONNECT',
                           'AUTOCONNECT-PRIORITY', 'TIMESTAMP-REAL'], 'connection', 'show')

    def get_wifi(self, rescan='no', ifname=None):
        args = ['device', 'wifi', 'list', '--rescan', rescan] + (['ifname', ifname] if ifname else [])
        return self.query(['IN-USE', 'BSSID', 'SSID', 'MODE', 'CHAN', 'RATE', 'SIGNAL', 'BARS', 'SECURITY'], *args)

    def get_connection(self, uuid, secrets=False):
        return self.show(*(['-s'] if secrets else []), 'connection', 'show', 'uuid', uuid)

    def wifi_devices(self):
        try:
            return [dev['DEVICE'] for dev in self.get_devices() if dev['TYPE'] == 'wifi']
        except RuntimeError:
            return []

    # ------------------------------------------------------------------
    # Jobs: general / radio
    # ------------------------------------------------------------------

    def execute_status(self, data):
        return self.success(status=self.get_status(), hostname=self.get_hostname())

    def execute_set_hostname(self, data):
        hostname = str(data.get('hostname') or '').strip()
        if not hostname:
            return self.fail("hostname fehlt")
        return self.result(self.run('general', 'hostname', hostname))

    def execute_networking(self, data):
        return self.result(self.run('networking', 'on' if yes_no(data.get('state', 'on')) == 'yes' else 'off'))

    def execute_connectivity(self, data):
        res = self.run('networking', 'connectivity', 'check')
        return self.result(res, connectivity=res.out.strip())

    def execute_radio(self, data):
        kind = data.get('kind', 'wifi')
        if kind not in ('wifi', 'wwan', 'all'):
            return self.fail("kind muss wifi, wwan oder all sein")
        return self.result(self.run('radio', kind, 'on' if yes_no(data.get('state', 'on')) == 'yes' else 'off'))

    def execute_permissions(self, data):
        return self.success(permissions={e['PERMISSION']: e['VALUE'] for e in self.get_permissions()})

    # ------------------------------------------------------------------
    # Jobs: device
    # ------------------------------------------------------------------

    def execute_devices(self, data):
        return self.success(devices=self.get_devices())

    def execute_device_show(self, data):
        return self.success(props=self.show('device', 'show', data['device']))

    def execute_device_connect(self, data):
        return self.result(self.run('device', 'connect', data['device'], timeout=90))

    def execute_device_disconnect(self, data):
        return self.result(self.run('device', 'disconnect', data['device']))

    def execute_device_reapply(self, data):
        return self.result(self.run('device', 'reapply', data['device']))

    def execute_device_delete(self, data):
        return self.result(self.run('device', 'delete', data['device']))

    def execute_device_set(self, data):
        key = data.get('key', 'autoconnect')
        if key not in ('autoconnect', 'managed'):
            return self.fail("key muss autoconnect oder managed sein")
        return self.result(self.run('device', 'set', data['device'], key, yes_no(data.get('value', 'yes'))))

    # ------------------------------------------------------------------
    # Jobs: wifi
    # ------------------------------------------------------------------

    def execute_wifi_list(self, data):
        rescan = 'yes' if yes_no(data.get('rescan', 'no')) == 'yes' else 'no'
        return self.success(networks=self.get_wifi(rescan, data.get('ifname') or None))

    def execute_wifi_rescan(self, data):
        return self.result(self.run('device', 'wifi', 'rescan', timeout=60))

    def execute_wifi_connect(self, data):
        ssid = str(data.get('ssid') or '').strip()
        if not ssid:
            return self.fail("ssid fehlt")
        args = ['device', 'wifi', 'connect', data.get('bssid') or ssid]
        if data.get('password'):
            args += ['password', data['password']]
        if data.get('ifname'):
            args += ['ifname', data['ifname']]
        if yes_no(data.get('hidden', 'no')) == 'yes':
            args += ['hidden', 'yes']
        if data.get('name'):
            args += ['name', data['name']]
        return self.result(self.run(*args, timeout=90))

    def execute_wifi_hotspot(self, data):
        args = ['device', 'wifi', 'hotspot']
        for key in ('ifname', 'ssid', 'band', 'channel', 'password'):
            if data.get(key):
                args += [key, data[key]]
        if data.get('con_name'):
            args += ['con-name', data['con_name']]
        return self.result(self.run(*args, timeout=60))

    def execute_wifi_password(self, data):
        return self.success(props=self.show('-m', 'multiline', 'device', 'wifi', 'show-password'))

    # ------------------------------------------------------------------
    # Jobs: connection
    # ------------------------------------------------------------------

    def execute_connections(self, data):
        return self.success(connections=self.get_connections())

    def execute_con_show(self, data):
        return self.success(props=self.get_connection(data['uuid'], yes_no(data.get('secrets', 'no')) == 'yes'))

    def execute_con_up(self, data):
        args = ['connection', 'up', 'uuid', data['uuid']] + (['ifname', data['ifname']] if data.get('ifname') else [])
        return self.result(self.run(*args, timeout=120))

    def execute_con_down(self, data):
        return self.result(self.run('connection', 'down', 'uuid', data['uuid']))

    def execute_con_delete(self, data):
        return self.result(self.run('connection', 'delete', 'uuid', data['uuid']))

    def execute_con_export(self, data):
        res = self.run('connection', 'export', 'uuid', data['uuid'])
        return self.result(res, config=res.out)

    def execute_con_clone(self, data):
        name = str(data.get('name') or '').strip()
        if not name:
            return self.fail("name fehlt")
        return self.result(self.run('connection', 'clone', 'uuid', data['uuid'], name))

    def execute_con_reload(self, data):
        return self.result(self.run('connection', 'reload'))

    def execute_con_import(self, data):
        vpn_type = data.get('vpn_type', 'wireguard')
        if not data.get('file'):
            return self.fail("file fehlt (Pfad auf dem Server)")
        return self.result(self.run('connection', 'import', 'type', vpn_type, 'file', data['file']))

    def execute_con_modify(self, data):
        """ Einzelne Eigenschaft (key/value) oder mehrere als 'set.<eigenschaft>' (nur geaenderte werden gesetzt) """
        props = self.changed_props(data)
        if data.get('key'):
            props.append((data['key'], str(data.get('value') or '')))
        if not props:
            return self.success("Keine Änderungen")
        args = ['connection', 'modify', 'uuid', data['uuid']]
        for key, value in props:
            args += [key, value]
        res = self.run(*args)
        if res.ok and yes_no(data.get('apply', 'no')) == 'yes':
            res = self.run('connection', 'up', 'uuid', data['uuid'], timeout=120)
        return self.result(res)

    def changed_props(self, data):
        """ (Eigenschaft, Wert) aus 'set.<eigenschaft>', die sich vom aktuellen Wert unterscheiden """
        sets = {k[4:]: str(v if v is not None else '') for k, v in data.items() if k.startswith('set.')}
        if not sets:
            return []
        current = self.get_connection(data['uuid'])
        props = []
        for key, value in sets.items():
            if self.EDIT_PROPS.get(key, (None, None, None))[2] == 'secret' and value == '':
                continue
            if value.strip() != self.plain(current.get(key, '')):
                props.append((key, value.strip()))
        return props

    @staticmethod
    def plain(value):
        """ nmcli zeigt leere Werte als '--' """
        return '' if value in ('--', '<hidden>') else value

    def execute_con_add(self, data):
        con_type = data.get('con_type', 'ethernet')
        args = ['connection', 'add', 'type', con_type]
        if data.get('name'):
            args += ['con-name', data['name']]
        if data.get('ifname'):
            args += ['ifname', data['ifname']]
        for key in ('ipv4.method', 'ipv4.addresses', 'ipv4.gateway', 'ipv4.dns'):
            value = data.get(key.replace('.', '_'))
            if value:
                args += [key, value]
        try:
            args += shlex.split(data.get('extra') or '')
        except ValueError as e:
            return self.fail(f"Weitere Eigenschaften nicht lesbar: {e}")
        return self.result(self.run(*args))

    # ------------------------------------------------------------------
    # UI Hilfen
    # ------------------------------------------------------------------

    def btn(self, title, op, hint, **params):
        return self.action_btn(dict(type=self.type, op=op, title=title, **params), hint=hint)

    def confirm_btn(self, title, op, hint, question, **params):
        """ Button mit Rueckfrage, fuer Loeschen und Aktionen, die die Verbindung kappen koennen """
        data = json.dumps(dict(type=self.type, op=op, **params))
        return w.button_js(title, f'if (confirm({json.dumps(question)})) exec_job({data});', hint=hint)

    def p_btn(self, title, data, hint):
        """ Button, der Werte aus Eingabefeldern ('#id') mitsendet """
        return self.action_btn_parametric(title, dict(type=self.type, **data), hint=hint)

    def nav(self, p: base.Page):
        p.div(' | '.join([w.a("Übersicht", self.link(self.part_index)),
                          w.a("WLAN", self.link(self.part_wifi)),
                          w.a("Neue Verbindung", self.link(self.part_add)),
                          w.a("Letzte Ausgabe", self.link(self.part_output))]))

    def part_error(self, p: base.Page, e):
        p(w.alert_error(esc(e)))

    # ------------------------------------------------------------------
    # UI: Uebersicht
    # ------------------------------------------------------------------

    def part_index(self, p: base.Page, params={}):
        self.nav(p)
        self.part_status(p)
        self.part_devices(p, params)
        self.part_connections(p, params)

    def part_status(self, p: base.Page):
        with p.section(h="Status"):
            try:
                status = self.get_status()
            except RuntimeError as e:
                return self.part_error(p, e)
            for key, value in status.items():
                p.prop(esc(key), esc(value))
            self.part_hostname(p)
            p.div(self.btn("Konnektivität prüfen", 'connectivity',
                           "Prüft, ob das Internet erreichbar ist (nmcli networking connectivity check)")
                  + self.btn("Netzwerk ein", 'networking', "Schaltet die Netzwerkverwaltung ein", state='on')
                  + self.confirm_btn("Netzwerk aus", 'networking',
                                     "Trennt alle Verbindungen - bei Fernzugriff ist das Gerät danach nicht erreichbar",
                                     "Wirklich alle Netzwerkverbindungen trennen?", state='off'))
            p.div(self.btn("WLAN ein", 'radio', "Schaltet den WLAN-Funk ein", kind='wifi', state='on')
                  + self.btn("WLAN aus", 'radio', "Schaltet den WLAN-Funk aus", kind='wifi', state='off')
                  + self.btn("WWAN ein", 'radio', "Schaltet Mobilfunk ein", kind='wwan', state='on')
                  + self.btn("WWAN aus", 'radio', "Schaltet Mobilfunk aus", kind='wwan', state='off'))
            with p.expandable("Berechtigungen", expanded=False):
                self.part_permissions(p)

    def part_hostname(self, p: base.Page):
        p.form_input('hostname', "Hostname", hint="Rechnername im Netzwerk (nmcli general hostname)",
                     id='nm_hostname', value=esc(self.get_hostname()))
        p.div(self.p_btn("Hostname setzen", dict(op='set_hostname', hostname='#nm_hostname'),
                         "Speichert den Hostnamen dauerhaft"))

    def part_permissions(self, p: base.Page):
        try:
            rows = self.get_permissions()
        except RuntimeError as e:
            return self.part_error(p, e)
        p.div("Was der Runner-Benutzer darf (yes / auth = Passwort nötig / no). Bei 'no' sudo=True verwenden.")
        p.grid(rows, [w.Grid.col('PERMISSION', "Berechtigung"), w.Grid.col('VALUE', "Wert")])

    def part_devices(self, p: base.Page, params):
        with p.section(h="Geräte"):
            try:
                devices = self.get_devices()
            except RuntimeError as e:
                return self.part_error(p, e)
            p.grid(devices, [
                w.Grid.col('DEVICE', "Gerät", html=True,
                           value=lambda e: w.a(esc(e['DEVICE']), self.link(self.part_device, device=e['DEVICE']))),
                w.Grid.col('TYPE', "Typ"), w.Grid.col('STATE', "Status"),
                w.Grid.col('CONNECTION', "Verbindung"),
                w.Grid.col('actions', '', html=True, sortable=False, value=self.device_actions),
            ], params, prefix='d_', empty="Keine Geräte")

    def device_actions(self, dev):
        name = dev['DEVICE']
        if dev['STATE'].startswith('connected') or dev['STATE'].startswith('verbunden'):
            res = self.confirm_btn("Trennen", 'device_disconnect', "Trennt das Gerät und verhindert Autoconnect",
                                   f"{name} trennen?", device=name)
        else:
            res = self.btn("Verbinden", 'device_connect', "Aktiviert das passende Profil auf dem Gerät", device=name)
        return res + self.btn("Reapply", 'device_reapply',
                              "Übernimmt geänderte Profileinstellungen ohne Verbindungsabbruch", device=name)

    def part_connections(self, p: base.Page, params):
        with p.section(h="Verbindungen"):
            try:
                cons = self.get_connections()
            except RuntimeError as e:
                return self.part_error(p, e)
            p.grid(cons, [
                w.Grid.col('NAME', "Name", html=True,
                           value=lambda e: w.a(esc(e['NAME']), self.link(self.part_connection, uuid=e['UUID']))),
                w.Grid.col('TYPE', "Typ"), w.Grid.col('DEVICE', "Gerät"), w.Grid.col('ACTIVE', "Aktiv"),
                w.Grid.col('AUTOCONNECT', "Autoconnect"), w.Grid.col('TIMESTAMP-REAL', "Zuletzt aktiv"),
                w.Grid.col('actions', '', html=True, sortable=False, value=self.connection_actions),
            ], params, search=True, prefix='c_', empty="Keine Verbindungsprofile")
            p.div(self.btn("Neu laden", 'con_reload', "Liest manuell geänderte Profildateien neu ein (connection reload)")
                  + w.button_js("Neue Verbindung", f'location.href = {json.dumps(self.link(self.part_add))};',
                                hint="Neues Verbindungsprofil anlegen oder VPN importieren"))

    def connection_actions(self, con):
        uuid = con['UUID']
        if con['ACTIVE'] == 'yes':
            return self.confirm_btn("Deaktivieren", 'con_down', "Trennt diese Verbindung",
                                    f"Verbindung {con['NAME']} trennen?", uuid=uuid)
        return self.btn("Aktivieren", 'con_up', "Aktiviert dieses Profil (connection up)", uuid=uuid)

    # ------------------------------------------------------------------
    # UI: Geraet
    # ------------------------------------------------------------------

    def part_device(self, p: base.Page, params={}):
        name = params.get('device', '')
        self.nav(p)
        with p.section(h=f"Gerät {esc(name)}"):
            p.div(self.btn("Verbinden", 'device_connect', "Aktiviert das passende Profil", device=name)
                  + self.confirm_btn("Trennen", 'device_disconnect', "Trennt das Gerät", f"{name} trennen?",
                                     device=name)
                  + self.btn("Reapply", 'device_reapply', "Übernimmt geänderte Einstellungen ohne Trennen",
                             device=name))
            p.div(self.btn("Managed ein", 'device_set', "NetworkManager verwaltet das Gerät", device=name,
                           key='managed', value='yes')
                  + self.btn("Managed aus", 'device_set', "Gerät nicht mehr durch NetworkManager verwalten",
                             device=name, key='managed', value='no')
                  + self.btn("Autoconnect ein", 'device_set', "Gerät verbindet sich automatisch", device=name,
                             key='autoconnect', value='yes')
                  + self.btn("Autoconnect aus", 'device_set', "Kein automatisches Verbinden", device=name,
                             key='autoconnect', value='no')
                  + self.confirm_btn("Löschen", 'device_delete', "Nur für Software-Geräte (Bridge, VLAN, Bond, ...)",
                                     f"Gerät {name} löschen?", device=name))
        with p.section(h="Eigenschaften"):
            try:
                props = self.show('device', 'show', name)
            except RuntimeError as e:
                return self.part_error(p, e)
            self.part_props(p, props, params)
        with p.expandable("LLDP-Nachbarn", expanded=False):
            res = self.nmcli('device', 'lldp', 'list', 'ifname', name)
            p.pre(esc(res.message() or "Keine LLDP-Informationen"))

    def part_props(self, p: base.Page, props, params):
        rows = [dict(key=k, value=v) for k, v in props.items()]
        p.grid(rows, [w.Grid.col('key', "Eigenschaft"), w.Grid.col('value', "Wert")], params, search=True,
               prefix='p_')

    # ------------------------------------------------------------------
    # UI: Verbindung
    # ------------------------------------------------------------------

    def part_connection(self, p: base.Page, params={}):
        uuid = params.get('uuid', '')
        secrets = params.get('secrets') == '1'
        self.nav(p)
        try:
            props = self.get_connection(uuid, secrets)
        except RuntimeError as e:
            return self.part_error(p, e)
        name = props.get('connection.id', uuid)
        with p.section(h=f"Verbindung {esc(name)}"):
            p.prop("UUID", esc(uuid))
            p.prop("Typ", esc(props.get('connection.type', '')))
            p.prop("Aktiv auf", esc(self.plain(props.get('GENERAL.DEVICES', '')) or '-'))
            self.part_connection_actions(p, uuid, name, props)
        with p.section(h="Einstellungen"):
            self.part_connection_form(p, uuid, props)
        with p.section(h="Eigenschaft setzen"):
            self.part_connection_prop(p, uuid, props)
        with p.expandable("Alle Eigenschaften", expanded=False):
            p.div(w.a("Mit Geheimnissen anzeigen", self.link(self.part_connection, uuid=uuid, secrets='1'))
                  if not secrets else "Geheimnisse werden angezeigt")
            self.part_props(p, props, params)

    def part_connection_actions(self, p: base.Page, uuid, name, props):
        p.div(self.btn("Aktivieren", 'con_up', "Aktiviert das Profil (connection up)", uuid=uuid)
              + self.confirm_btn("Deaktivieren", 'con_down', "Trennt die Verbindung", f"{name} trennen?", uuid=uuid)
              + self.confirm_btn("Löschen", 'con_delete', "Löscht das Profil dauerhaft",
                                 f"Profil {name} wirklich löschen?", uuid=uuid))
        p.form_input('name', "Name der Kopie", hint="Name für das geklonte Profil", id='nm_clone_name',
                     value=esc(f"{name} Kopie"))
        p.div(self.p_btn("Klonen", dict(op='con_clone', uuid=uuid, name='#nm_clone_name'),
                         "Legt eine Kopie des Profils mit neuer UUID an"))
        if props.get('connection.type') in ('vpn', 'wireguard'):
            p.div(self.p_btn("Exportieren", dict(op='con_export', uuid=uuid),
                             "Gibt die VPN-Konfiguration aus (siehe Ergebnis unten)"))
        p.pre('', id='result')

    def part_connection_form(self, p: base.Page, uuid, props):
        """ Formular fuer die gaengigen Eigenschaften, gesendet werden nur geaenderte """
        data = dict(op='con_modify', uuid=uuid)
        for i, (key, (title, hint, options)) in enumerate(self.EDIT_PROPS.items()):
            if key not in props:
                continue
            hid = f'nm_set_{i}'
            data['set.' + key] = '#' + hid
            value = self.plain(props[key])
            if options == 'secret':
                p.form_input(hid, title, hint=f"{key}: {hint}", id=hid, type='password', value='')
            elif options:
                p.form_select(hid, title, options, value, hint=f"{key}: {hint}", id=hid)
            else:
                p.form_input(hid, title, hint=f"{key}: {hint}", id=hid, value=esc(value))
        p.div(self.p_btn("Speichern", data, "Speichert geänderte Werte im Profil (wirkt bei nächster Aktivierung)")
              + self.p_btn("Speichern und anwenden", {**data, 'apply': 'yes'},
                           "Speichert und aktiviert das Profil neu - kurze Unterbrechung"))

    def part_connection_prop(self, p: base.Page, uuid, props):
        p.div("Beliebige Eigenschaft ändern. Mit '+' vor dem Namen wird ein Wert hinzugefügt (z.B. +ipv4.dns), "
              "mit '-' entfernt. Liste aller Eigenschaften: man nm-settings-nmcli.")
        p.form_input('key', "Eigenschaft", hint="z.B. ipv4.routes, +ipv4.dns, 802-3-ethernet.wake-on-lan",
                     id='nm_prop_key', suggestions=[k for k in props if k[:1].islower()])
        p.form_input('value', "Wert", hint="Neuer Wert, leer = Eigenschaft zurücksetzen", id='nm_prop_value')
        p.div(self.p_btn("Setzen", dict(op='con_modify', uuid=uuid, key='#nm_prop_key', value='#nm_prop_value'),
                         "nmcli connection modify <uuid> <eigenschaft> <wert>"))

    # ------------------------------------------------------------------
    # UI: WLAN
    # ------------------------------------------------------------------

    def part_wifi(self, p: base.Page, params={}):
        self.nav(p)
        with p.section(h="WLAN-Netze"):
            p.div(self.btn("Neu suchen", 'wifi_rescan', "Startet einen neuen Scan (dauert einige Sekunden)")
                  + self.btn("WLAN ein", 'radio', "Schaltet den WLAN-Funk ein", kind='wifi', state='on'))
            try:
                networks = [n for n in self.get_wifi() if n['SSID']]
            except RuntimeError as e:
                return self.part_error(p, e)
            p.grid(networks, [
                w.Grid.col('IN-USE', "", width='20px'), w.Grid.col('SSID', "SSID"),
                w.Grid.col('SIGNAL', "Signal", sort=lambda e: int(e['SIGNAL'] or 0)),
                w.Grid.col('BARS', ""), w.Grid.col('SECURITY', "Sicherheit"), w.Grid.col('CHAN', "Kanal"),
                w.Grid.col('RATE', "Rate"), w.Grid.col('BSSID', "BSSID"),
                w.Grid.col('actions', '', html=True, sortable=False,
                           value=lambda e: w.button_js("Auswählen", f'nm_wifi_select({json.dumps(e["SSID"])});',
                                                       hint="Übernimmt die SSID ins Formular \"Verbinden\"")),
            ], params, search=True, prefix='w_', empty="Keine Netze gefunden")
        self.part_wifi_connect(p)
        self.part_hotspot(p)
        with p.expandable("Aktives WLAN-Passwort", expanded=False):
            p.div(self.p_btn("Anzeigen", dict(op='wifi_password'),
                             "Zeigt SSID und Passwort des aktiven WLAN bzw. Hotspots (Ergebnis unten)"))
        p.pre('', id='result')

    def ifname_select(self, p: base.Page, hid, hint):
        options = {'': "(automatisch)", **{dev: dev for dev in self.wifi_devices()}}
        p.form_select(hid, "WLAN-Gerät", options, '', hint=hint, id=hid)

    def part_wifi_connect(self, p: base.Page):
        with p.section(h="Verbinden"):
            p.form_input('ssid', "SSID", hint="Name des WLAN (aus der Liste auswählen oder eintippen)", id='nm_wifi_ssid')
            p.form_input('password', "Passwort", hint="WPA-Schlüssel, leer bei offenen Netzen", id='nm_wifi_pw',
                         type='password')
            self.ifname_select(p, 'nm_wifi_if', "Über welches Gerät verbunden wird")
            p.form_input('hidden', "Versteckt", hint="Für Netze, deren SSID nicht ausgestrahlt wird",
                         id='nm_wifi_hidden', type='checkbox')
            p.div(self.p_btn("Verbinden", dict(op='wifi_connect', ssid='#nm_wifi_ssid', password='#nm_wifi_pw',
                                               ifname='#nm_wifi_if', hidden='#nm_wifi_hidden'),
                             "Legt ein Profil an und verbindet (nmcli device wifi connect)"))
            p.js_ready('window.nm_wifi_select = function(ssid) { document.getElementById("nm_wifi_ssid").value = ssid;'
                       ' document.getElementById("nm_wifi_pw").focus(); };')

    def part_hotspot(self, p: base.Page):
        with p.section(h="Hotspot"):
            p.div("Erzeugt einen Access Point und teilt die bestehende Verbindung. Die WLAN-Verbindung des Geräts "
                  "wird dabei getrennt.")
            p.form_input('ssid', "SSID", hint="Name des Hotspots", id='nm_hs_ssid', value='nxhotspot')
            p.form_input('password', "Passwort", hint="Mindestens 8 Zeichen, leer = zufälliges Passwort",
                         id='nm_hs_pw', type='password')
            p.form_select('band', "Band", {'': "(automatisch)", 'bg': "2,4 GHz (bg)", 'a': "5 GHz (a)"}, '',
                          hint="Frequenzband des Hotspots", id='nm_hs_band')
            self.ifname_select(p, 'nm_hs_if', "Gerät, das den Hotspot aufspannt")
            p.div(self.p_btn("Hotspot starten", dict(op='wifi_hotspot', ssid='#nm_hs_ssid', password='#nm_hs_pw',
                                                     band='#nm_hs_band', ifname='#nm_hs_if'),
                             "nmcli device wifi hotspot - beenden über 'Deaktivieren' der Verbindung Hotspot"))

    # ------------------------------------------------------------------
    # UI: Neue Verbindung / Import
    # ------------------------------------------------------------------

    def part_add(self, p: base.Page, params={}):
        self.nav(p)
        with p.section(h="Neue Verbindung"):
            types = {k: f"{v[0]} ({k})" for k, v in self.CON_TYPES.items()}
            p.form_select('con_type', "Typ", types, 'ethernet', hint="Art der Verbindung (nmcli connection add type)",
                          id='nm_add_type')
            p.form_input('name', "Name", hint="Name des Profils, leer = automatisch", id='nm_add_name')
            p.form_input('ifname', "Interface", hint="z.B. eth0, br0, wlan0 - leer = beliebiges Gerät",
                         id='nm_add_if')
            p.form_select('ipv4_method', "IPv4", self.IPV4_METHODS, '', hint="Wie die IPv4-Adresse bezogen wird",
                          id='nm_add_m')
            p.form_input('ipv4_addresses', "Adresse", hint="Nur bei manuell: z.B. 192.168.1.10/24", id='nm_add_ip')
            p.form_input('ipv4_gateway', "Gateway", hint="Nur bei manuell: z.B. 192.168.1.1", id='nm_add_gw')
            p.form_input('ipv4_dns', "DNS", hint="z.B. 1.1.1.1,9.9.9.9", id='nm_add_dns')
            p.form_input('extra', "Weitere", hint="Weitere Eigenschaften als 'name wert' Paare, Beispiel je nach Typ",
                         id='nm_add_extra', placeholder=esc(self.CON_TYPES['ethernet'][1]), style='width: 60%')
            p.div(self.p_btn("Anlegen", dict(op='con_add', con_type='#nm_add_type', name='#nm_add_name',
                                             ifname='#nm_add_if', ipv4_method='#nm_add_m', ipv4_addresses='#nm_add_ip',
                                             ipv4_gateway='#nm_add_gw', ipv4_dns='#nm_add_dns', extra='#nm_add_extra'),
                             "Legt das Profil an (nmcli connection add)"))
            hints = json.dumps({k: v[1] for k, v in self.CON_TYPES.items()})
            p.js_ready('document.getElementById("nm_add_type").addEventListener("change", function(e) {'
                       f' document.getElementById("nm_add_extra").placeholder = ({hints})[e.target.value] || ""; }});')
        self.part_import(p)
        p.pre('', id='result')

    def part_import(self, p: base.Page):
        with p.section(h="VPN importieren"):
            p.form_select('vpn_type', "Typ", self.VPN_IMPORT_TYPES, 'wireguard',
                          hint="Format der Datei, außer wireguard wird das passende NetworkManager-VPN-Plugin benötigt",
                          id='nm_imp_type')
            p.form_input('file', "Datei", hint="Pfad auf diesem Rechner, z.B. /etc/wireguard/wg0.conf oder client.ovpn",
                         id='nm_imp_file')
            p.div(self.p_btn("Importieren", dict(op='con_import', vpn_type='#nm_imp_type', file='#nm_imp_file'),
                             "nmcli connection import type <typ> file <datei>"))

    # ------------------------------------------------------------------
    # UI: Ausgabe
    # ------------------------------------------------------------------

    def part_output(self, p: base.Page, params={}):
        self.nav(p)
        with p.section(h="Letzte nmcli-Ausgabe"):
            p.prop("Kommando", esc(self.last_cmd or '-'))
            p.pre(esc(self.last_output[-5000:] or "Noch keine Aktion ausgeführt"))
