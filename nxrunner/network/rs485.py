"""
Netzwerk ueber 2-Draht-RS-485 (halbduplex), auch fuer schlechte Kabelverbindungen

PPP braucht eine Vollduplex-Leitung. Auf einem RS-485-Paar darf aber immer nur einer senden, deshalb nutzt
dieser Runner ein eigenes Polling-Protokoll (nxrunner.network.rs485_proto) und ein TUN-Interface:

  - edge (Master, Adresse 1): fragt die clients der Reihe nach ab, teilt sein Internet (IP-Forwarding + NAT)
  - client (Slave, Adresse 2..254): sendet nur, wenn der edge ihn abfragt; Default-Route ueber das TUN-Interface,
            optional WLAN-Hotspot (wifi_share=True) wie bei NetworkOverSerial
  - mehrere clients am selben Bus sind moeglich (edge: clients="2,3")

  IP-Adressen: <subnet>.1 (edge) und <subnet>.<address> (client), Standard 10.98.0.0/24.
  Gegen Leitungsfehler: CRC32, kleine Fragmente mit selektiver Wiederholung, Fragmentgroesse passt sich an.
  Durchsatz: bei 115200 Baud etwa 5-8 KB/s fuer beide Richtungen zusammen.

    Config (runners):
        network_rs485: nxrunner.network.rs485:NetworkOverRS485(role="edge", device="/dev/ttyUSB0", baud=115200)
        network_rs485: nxrunner.network.rs485:NetworkOverRS485(role="client", address=2, device="/dev/ttyUSB0",
                            autostart=True, wifi_share=True, wifi_ssid="nxrs485", wifi_password="geheim123")

    Job:
        {"type": "network_rs485", "op": "start"}
        {"type": "network_rs485", "op": "stop"}
        {"type": "network_rs485", "op": "status"}
        {"type": "network_rs485", "op": "check"}

    Voraussetzungen (Linux): /dev/net/tun, Rechte ueber sudo -n (ip, sysctl, iptables/nft, nmcli) oder root,
    Lese-/Schreibzugriff auf den Serial-Port (Gruppe dialout oder root). Beide Seiten mit derselben Baudrate.
"""
import atexit
import collections
import os
import select
import struct
import threading
import time

from nwebclient import runner as r
from nwebclient import base
from nwebclient import web as w
from nwebclient.runner import op

from nxrunner.network.rs485_proto import ArqLink, BusMaster, BusSlave, BusTiming, MASTER_ADDR
from nxrunner.network.serial import (Cmd, Check, InternetSharing, SerialNetChecks, WifiShare, esc, yes, which,
                                     serial_devices, format_bytes, part_checks, part_form_wifi, BAUDS)

ROLES = {'client': "client (Slave, nutzt das Internet des edge)", 'edge': "edge (Master, teilt sein Internet)"}
RS_KEYS = ('role', 'device', 'baud', 'address', 'clients', 'wifi_share', 'wifi_ssid', 'wifi_password', 'wifi_ifname')
TUNSETIFF, IFF_TUN, IFF_NO_PI = 0x400454ca, 0x0001, 0x1000


def termios_baud(baud):
    import termios
    speed = getattr(termios, f'B{int(baud)}', None)
    if speed is None:
        raise ValueError(f"Baudrate {baud} wird von termios nicht unterstützt")
    return speed


class SerialPort:
    """ Serial-Port im Raw-Modus 8N1 ohne Flusssteuerung (nur Linux, ohne pyserial) """

    def __init__(self, device, baud):
        import termios
        import fcntl
        self.fd = os.open(device, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
        try:
            fcntl.ioctl(self.fd, termios.TIOCEXCL)  # kein zweiter Prozess auf dem Port
            attrs = termios.tcgetattr(self.fd)
            attrs[0] = attrs[1] = attrs[3] = 0  # iflag, oflag, lflag: raw
            attrs[2] = termios.CS8 | termios.CREAD | termios.CLOCAL
            attrs[4] = attrs[5] = termios_baud(baud)
            attrs[6][termios.VMIN], attrs[6][termios.VTIME] = 0, 0
            termios.tcsetattr(self.fd, termios.TCSANOW, attrs)
            termios.tcflush(self.fd, termios.TCIOFLUSH)
        except Exception:
            os.close(self.fd)
            raise

    def read(self, timeout) -> bytes:
        ready, _, _ = select.select([self.fd], [], [], max(0.0, timeout))
        if not ready:
            return b''
        try:
            return os.read(self.fd, 4096)
        except BlockingIOError:
            return b''

    def write(self, data: bytes):
        import termios
        view = memoryview(data)
        while view:
            select.select([], [self.fd], [], 1.0)
            try:
                view = view[os.write(self.fd, view):]
            except BlockingIOError:
                continue
        termios.tcdrain(self.fd)

    def close(self):
        try:
            os.close(self.fd)
        except OSError:
            pass


class Tun:
    """ TUN-Interface; ohne root wird es per sudo ip tuntap fuer den aktuellen Benutzer angelegt """

    def __init__(self, name, cmd: Cmd):
        self.name, self.cmd, self.fd = name, cmd, None

    def open(self):
        import fcntl
        if self.cmd.sudo:
            self.cmd.run('ip', 'tuntap', 'add', 'dev', self.name, 'mode', 'tun', 'user', os.getuid())
        self.fd = os.open('/dev/net/tun', os.O_RDWR)
        fcntl.ioctl(self.fd, TUNSETIFF, struct.pack('16sH', self.name.encode(), IFF_TUN | IFF_NO_PI))

    def configure(self, addr, mtu):
        steps = [('sysctl', '-w', f'net.ipv6.conf.{self.name}.disable_ipv6=1'),  # spart Bandbreite
                 ('ip', 'addr', 'replace', f'{addr}/24', 'dev', self.name),
                 ('ip', 'link', 'set', 'dev', self.name, 'mtu', mtu, 'up')]
        for step in steps:
            ok, out = self.cmd.run(*step)
            if not ok and step[0] == 'ip':
                raise RuntimeError(' '.join(map(str, step[:3])) + ": " + out)

    def read(self, timeout):
        ready, _, _ = select.select([self.fd], [], [], timeout)
        return os.read(self.fd, 65535) if ready else None

    def write(self, packet: bytes):
        try:
            os.write(self.fd, packet)
        except OSError:
            pass

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        self.cmd.run('ip', 'link', 'del', self.name)


class NetworkOverRS485(InternetSharing, r.BaseJobExecutor):
    """
    IP-Netz über 2-Draht-RS-485 mit Master/Slave-Polling. edge teilt das Internet (NAT), client nutzt es.
    """

    LOG_LINES = 200

    def __init__(self, type='network_rs485', role='client', device='/dev/ttyUSB0', baud=115200, address=2,
                 clients='2', subnet='10.98.0', ifname='nxrs485', mtu=1500, window=8, frag=96, frag_min=24,
                 frag_max=240, max_tries=16, guard_ms=0, reply_ms=80, wan=None, default_route=True,
                 route_metric=10, sudo=True, autostart=False, wifi_share=False, wifi_ssid='nxrs485',
                 wifi_password='', wifi_ifname='', wifi_band=''):
        super().__init__(type)
        self.role, self.device, self.baud = role, device, int(baud)
        self.address, self.clients, self.subnet, self.ifname = int(address), str(clients), subnet, ifname
        self.mtu, self.wan, self.nft_table = int(mtu), wan, 'nxrs485'
        self.arq = dict(window=window, frag=frag, frag_min=frag_min, frag_max=frag_max, max_tries=max_tries)
        self.guard_ms, self.reply_ms = float(guard_ms), float(reply_ms)
        self.default_route, self.route_metric = yes(default_route), int(route_metric)
        self.wifi_share, self.wifi_ssid, self.wifi_password = yes(wifi_share), wifi_ssid, wifi_password or ''
        self.wifi_ifname, self.wifi_band, self.wifi_state = wifi_ifname or '', wifi_band or '', ''
        self.cmd = Cmd(sudo, log=self.info)
        self.lock = threading.RLock()
        self.log = collections.deque(maxlen=self.LOG_LINES)
        self.port = self.tun = self.bus = None
        self.links, self.threads = {}, []
        self.running, self.started, self.last_error, self.route_set = False, 0, '', False
        self.define_vars('role', 'device', 'baud', 'address', 'clients', 'last_error')
        atexit.register(self.release_on_exit)
        if yes(autostart):
            threading.Timer(2, self.start).start()

    # ------------------------------------------------------------------
    # Eigenschaften
    # ------------------------------------------------------------------

    @property
    def share_src(self):
        return f'{self.subnet}.0/24'

    @property
    def local_ip(self):
        return f'{self.subnet}.{MASTER_ADDR if self.role == "edge" else self.address}'

    @property
    def wifi(self):
        if self.role != 'client' or not self.wifi_share:
            return None
        return WifiShare(self.cmd, self.wifi_ssid, self.wifi_password, self.wifi_ifname, self.wifi_band)

    def client_addresses(self) -> list:
        res = []
        for part in self.clients.replace(';', ',').split(','):
            if part.strip().isdigit() and 1 < int(part) < 255:
                res.append(int(part))
        return res or [2]

    def note(self, msg):
        self.log.append(time.strftime('%H:%M:%S ') + msg)
        self.info(msg)

    def config(self) -> dict:
        return {key: getattr(self, key) for key in RS_KEYS}

    # ------------------------------------------------------------------
    # Start / Stop
    # ------------------------------------------------------------------

    def start(self):
        """ :return: (ok, nachricht) """
        with self.lock:
            if self.running:
                return True, "läuft bereits"
            failed = [c for c in self.checks() if c.level == 'fail']
            if failed:
                self.last_error = '; '.join(f"{c.name}: {c.message}" for c in failed)
                return False, "Voraussetzungen fehlen: " + self.last_error
            try:
                self.open_devices()
                self.setup_network()
                self.start_threads()
            except Exception as e:
                self.last_error = str(e)
                self.note("Start fehlgeschlagen: " + str(e))
                self.stop()
                return False, str(e)
            self.last_error = ''
            msg = f"RS-485 gestartet ({self.role}, {self.device} @ {self.baud}, {self.local_ip})"
            self.note(msg)
            return True, msg + self.start_wifi()

    def open_devices(self):
        self.port = SerialPort(self.device, self.baud)
        self.tun = Tun(self.ifname, self.cmd)
        self.tun.open()
        self.tun.configure(self.local_ip, self.mtu)

    def setup_network(self):
        if self.role == 'edge':
            self.enable_sharing()

    def start_threads(self):
        timing = BusTiming(self.baud, self.guard_ms, self.reply_ms)
        if self.role == 'edge':
            self.links = {a: ArqLink(a, self.tun.write, **self.arq) for a in self.client_addresses()}
            self.bus = BusMaster(self.port, self.links, timing)
        else:
            self.links = {MASTER_ADDR: ArqLink(MASTER_ADDR, self.tun.write, **self.arq)}
            self.bus = BusSlave(self.port, self.links[MASTER_ADDR], self.address, timing)
        self.running, self.started = True, time.time()
        self.threads = [threading.Thread(target=self.run_bus, daemon=True, name='rs485-bus'),
                        threading.Thread(target=self.run_tun, daemon=True, name='rs485-tun')]
        for t in self.threads:
            t.start()

    def run_bus(self):
        try:
            self.bus.run()
        except Exception as e:
            self.last_error = f"Bus: {e}"
            self.note(self.last_error)

    def run_tun(self):
        """ Pakete vom TUN-Interface in die Sendewarteschlange, client: Route nach Verbindungszustand """
        next_route_check = 0
        while self.running:
            try:
                packet = self.tun.read(0.5)
            except OSError as e:
                self.last_error = f"TUN: {e}"
                return
            if packet:
                self.route_packet(packet)
            if time.monotonic() >= next_route_check:
                self.update_route()
                next_route_check = time.monotonic() + 2

    def route_packet(self, packet: bytes):
        if len(packet) < 20 or packet[0] >> 4 != 4:
            return  # nur IPv4
        if self.role == 'edge':
            link = self.links.get(packet[19]) if packet[16:19] == self.subnet_bytes() else None
        else:
            link = self.links.get(MASTER_ADDR)
        if link is not None and link.enqueue(packet):
            self.bus.wake.set()

    def subnet_bytes(self) -> bytes:
        return bytes(int(x) for x in self.subnet.split('.'))

    def update_route(self):
        """ Default-Route nur, solange der edge antwortet, sonst bleiben andere Verbindungen nutzbar """
        if self.role != 'client' or not self.default_route:
            return
        alive = self.links[MASTER_ADDR].alive(timeout=10)
        if alive and not self.route_set:
            ok, out = self.cmd.run('ip', 'route', 'replace', 'default', 'dev', self.ifname, 'metric',
                                   self.route_metric)
            self.route_set = ok
            self.note("Default-Route über " + self.ifname + (" gesetzt" if ok else ": " + out))
        elif not alive and self.route_set:
            self.cmd.run('ip', 'route', 'del', 'default', 'dev', self.ifname)
            self.route_set = False
            self.note("edge antwortet nicht, Default-Route entfernt")

    def start_wifi(self) -> str:
        if self.wifi is None:
            return ''
        ok, self.wifi_state = self.wifi.start()
        if not ok:
            self.last_error = self.wifi_state
        return ', ' + self.wifi_state

    def stop(self):
        """ :return: (ok, nachricht) """
        with self.lock:
            self.running = False
            if self.bus is not None:
                self.bus.stop.set()
                self.bus.wake.set()
            for t in self.threads:
                t.join(timeout=3)
            self.threads, self.bus = [], None
            self.release_devices()
            if self.role == 'client' and which('nmcli'):
                WifiShare(self.cmd).stop()
                self.wifi_state = ''
            if self.nat_wan:
                self.disable_sharing()
            self.note("gestoppt")
            return True, f"{self.device} freigegeben"

    def release_devices(self):
        if self.port is not None:
            self.port.close()
            self.port = None
        if self.tun is not None:
            self.tun.close()  # entfernt auch die Route
            self.tun = None
        self.route_set = False

    def release_on_exit(self):
        try:
            if self.running:
                self.stop()
        except Exception:
            pass

    def configure(self, **cfg):
        """ Aendert Einstellungen aus RS_KEYS (leere Werte bleiben unveraendert), nur wenn gestoppt """
        if self.running:
            raise RuntimeError("Verbindung läuft, zuerst stoppen")
        cfg = {k: v for k, v in cfg.items() if k in RS_KEYS and v not in (None, '')}
        if 'role' in cfg and cfg['role'] not in ROLES:
            raise ValueError("role muss client oder edge sein")
        if 'address' in cfg and not (str(cfg['address']).isdigit() and 1 < int(cfg['address']) < 255):
            raise ValueError("address muss zwischen 2 und 254 liegen")
        if len(cfg.get('wifi_password', '12345678')) < 8:
            raise ValueError("WLAN-Passwort braucht mindestens 8 Zeichen")
        for key, value in cfg.items():
            if key in ('baud', 'address'):
                value = int(value)
            elif key == 'wifi_share':
                value = yes(value)
            setattr(self, key, value)

    # ------------------------------------------------------------------
    # Pruefungen
    # ------------------------------------------------------------------

    def checks(self) -> list:
        sc = SerialNetChecks(self.role, self.device, self.baud, wan=self.wan, cmd=self.cmd, wifi=self.wifi)
        checks = [sc.check_os(), sc.check_role(), self.check_baud(), self.check_tun(), sc.check_sudo(),
                  sc.check_device(), sc.check_device_free(), sc.check_getty(), sc.check_modemmanager()]
        if self.role == 'edge':
            checks += [sc.check_iptables(), sc.check_wan(), sc.check_internet()]
        if self.wifi is not None:
            checks += self.wifi.checks()
        return [c for c in checks if c is not None]

    def check_baud(self):
        try:
            termios_baud(self.baud)
            return Check("Baudrate", 'ok', str(self.baud), "Beide Seiten müssen dieselbe Baudrate nutzen")
        except ImportError:
            return Check("Baudrate", 'fail', "termios fehlt", "Nur unter Linux nutzbar")
        except ValueError as e:
            return Check("Baudrate", 'fail', str(e), "z.B. 115200 oder 57600")

    def check_tun(self):
        if os.path.exists('/dev/net/tun'):
            return Check("/dev/net/tun", 'ok', "TUN-Unterstützung vorhanden")
        return Check("/dev/net/tun", 'fail', "fehlt", "sudo modprobe tun")

    # ------------------------------------------------------------------
    # Operationen
    # ------------------------------------------------------------------

    @op("Verbindung über RS-485 aufbauen", result={'success': 'bool', 'message': 'str'}, name='start')
    def op_start(self):
        ok, msg = self.start()
        return self.success(msg, ui_reload=True) if ok else self.fail(msg)

    @op("Verbindung trennen und den Serial-Port freigeben", result={'success': 'bool', 'message': 'str'},
        name='stop')
    def op_stop(self):
        ok, msg = self.stop()
        return self.success(msg, ui_reload=True) if ok else self.fail(msg)

    @op("Verbindung neu aufbauen", result={'success': 'bool', 'message': 'str'})
    def restart(self):
        self.stop()
        return self.op_start()

    @op("Status und Statistik der Verbindung", success={'status': 'dict'}, fail={}, name='status')
    def op_status(self):
        return self.success(status=self.get_status())

    @op("Voraussetzungen prüfen", success={'checks': 'list', 'ok': 'bool'}, fail={}, name='check')
    def op_check(self):
        checks = self.checks()
        return self.success(ok=SerialNetChecks.ok(checks), checks=[c.to_dict() for c in checks])

    @op("Einstellungen ändern (nur gestoppt)", result={'success': 'bool', 'message': 'str'}, name='configure')
    def op_configure(self, role: str = '', device: str = '', baud: str = '', address: str = '', clients: str = '',
                     wifi_share: str = '', wifi_ssid: str = '', wifi_password: str = '', wifi_ifname: str = ''):
        try:
            self.configure(role=role, device=device, baud=baud, address=address, clients=clients,
                           wifi_share=wifi_share, wifi_ssid=wifi_ssid, wifi_password=wifi_password,
                           wifi_ifname=wifi_ifname)
        except (RuntimeError, ValueError) as e:
            return self.fail(str(e))
        return self.success("Gespeichert", ui_reload=True)

    def line_stats(self) -> dict:
        reader = self.bus.reader if self.bus is not None else None
        if reader is None:
            return {}
        return dict(frames_ok=reader.ok, frames_bad=reader.bad, error_rate=round(reader.error_rate(), 3))

    def get_status(self) -> dict:
        return dict(role=self.role, device=self.device, baud=self.baud, running=self.running,
                    ifname=self.ifname, local_ip=self.local_ip, nat_wan=self.nat_wan or '',
                    route=self.route_set, uptime=round(time.time() - self.started) if self.running else 0,
                    line=self.line_stats(), links=[link.to_dict() for link in self.links.values()],
                    wifi_state=self.wifi_state, last_error=self.last_error)

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def btn(self, title, op_name, hint, **params):
        return self.action_btn(dict(type=self.type, op=op_name, title=title, **params), hint=hint)

    def part_index(self, p: base.Page, params={}):
        self.part_state(p)
        if self.running:
            self.part_links(p)
            self.part_line(p)
        else:
            self.part_settings(p)
            with p.section(h="Voraussetzungen"):
                part_checks(p, self.checks())
        with p.expandable("Protokoll", expanded=False):
            p.pre(esc('\n'.join(self.log) or "Noch keine Einträge"))

    def part_state(self, p: base.Page):
        st = self.get_status()
        with p.section(h="Netzwerk über RS-485"):
            p.prop("Zustand", self.state_text())
            p.prop("Rolle", esc(ROLES.get(self.role, self.role)))
            p.prop("Gerät", esc(f"{self.device} @ {self.baud} Baud"))
            p.prop("Interface", esc(f"{self.ifname} {self.local_ip}/24"))
            for key, title in (('nat_wan', "Geteilt über"), ('wifi_state', "WLAN-Hotspot"),
                               ('last_error', "Letzter Fehler")):
                if st[key]:
                    p.prop(title, esc(st[key]))
            if self.role == 'client' and self.running:
                p.prop("Default-Route", "über " + esc(self.ifname) if self.route_set else "nicht gesetzt")
            p.div(self.btn("Verbinden", 'start', "Öffnet den Port, legt das TUN-Interface an und startet das Polling")
                  + self.btn("Trennen", 'stop', "Beendet die Verbindung, entfernt Interface, NAT und Route")
                  + self.btn("Neu verbinden", 'restart', "Trennt und baut die Verbindung neu auf"))

    def state_text(self) -> str:
        if not self.running:
            return "gestoppt"
        alive = [link for link in self.links.values() if link.alive()]
        return f"verbunden ({len(alive)}/{len(self.links)} Gegenstellen)" if alive else "läuft, keine Antwort"

    def part_links(self, p: base.Page):
        rows = [self.link_row(d) for d in self.get_status()['links']]
        with p.section(h="Gegenstellen"):
            p.grid(rows, [w.Grid.col('peer', "Gegenstelle"), w.Grid.col('state', "Zustand"),
                          w.Grid.col('tx', "Gesendet"), w.Grid.col('rx', "Empfangen"),
                          w.Grid.col('rate', "Durchsatz"), w.Grid.col('retx', "Wiederholungen"),
                          w.Grid.col('drops', "Verworfen"), w.Grid.col('frag', "Fragment")], empty="Keine")

    def link_row(self, d: dict) -> dict:
        seen = "nie" if d['last_seen'] is None else f"vor {d['last_seen']} s"
        return dict(peer=esc(f"{self.subnet}.{d['peer']} (Adr. {d['peer']})"),
                    state=("verbunden" if d['alive'] else "keine Antwort") + f", {seen}",
                    tx=f"{d['tx_packets']} Pakete, {format_bytes(d['tx_bytes'])}",
                    rx=f"{d['rx_packets']} Pakete, {format_bytes(d['rx_bytes'])}",
                    rate=f"↑ {format_bytes(d['rate_tx'])}/s ↓ {format_bytes(d['rate_rx'])}/s",
                    retx=f"{d['retransmits']} (Ø {d['retx_avg']} je Fragment)",
                    drops=f"{d['drop_tries']} Pakete, Warteschlange voll {d['drop_queue']}",
                    frag=f"{d['frag']} Byte")

    def part_line(self, p: base.Page):
        line = self.line_stats()
        with p.section(h="Leitung"):
            p.prop("Rahmen ok", str(line.get('frames_ok', 0)))
            p.prop("Rahmen defekt (CRC)", str(line.get('frames_bad', 0)))
            p.prop("Fehlerquote", f"{line.get('error_rate', 0) * 100:.1f} %")
            if line.get('error_rate', 0) > 0.2:
                p(w.alert_error("Sehr viele defekte Rahmen: Baudrate senken (z.B. 19200), Abschlusswiderstand "
                                "120 Ω an beiden Enden, A/B nicht vertauscht, Kabel prüfen"))

    def part_settings(self, p: base.Page):
        cfg = self.config()
        with p.section(h="Einstellungen"):
            p.form_select('role', "Rolle", ROLES, value=cfg['role'], id='rs_role',
                          hint="edge fragt den Bus ab und teilt sein Internet, client antwortet nur")
            devices = serial_devices()
            if cfg['device'] and cfg['device'] not in devices:
                devices.insert(0, cfg['device'])
            p.form_select('device', "Serial-Gerät", devices, value=cfg['device'], id='rs_device',
                          hint="RS-485-Adapter, /dev/serial/by-id/... bleibt nach Umstecken gleich")
            p.form_select('baud', "Baudrate", [b for b in BAUDS if b <= 921600], value=cfg['baud'], id='rs_baud',
                          hint="Auf beiden Seiten gleich; bei schlechtem Kabel niedriger wählen")
            p.form_input('address', "Adresse (client)", value=str(cfg['address']), id='rs_address',
                         hint="2 bis 254, je client am Bus eindeutig")
            p.form_input('clients', "Clients (edge)", value=esc(cfg['clients']), id='rs_clients',
                         hint="Adressen der clients, die der edge abfragt, z.B. 2 oder 2,3")
            part_form_wifi(p, 'rs_', cfg)
            job = dict(type=self.type, op='configure', **{key: '#rs_' + key for key in RS_KEYS})
            p.div(self.action_btn_parametric("Übernehmen", job, hint="Speichert die Einstellungen (nur gestoppt)"))
