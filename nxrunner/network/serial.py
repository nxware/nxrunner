"""
Netzwerk ueber eine serielle Verbindung (PPP)

Zwei Rechner werden ueber eine serielle Leitung (USB-Seriell-Adapter, UART, Nullmodem) mit pppd verbunden.

  - edge:   hat Internet und teilt es mit dem client (IP-Forwarding + NAT/MASQUERADE ueber iptables)
  - client: bekommt ueber den edge Internet (Default-Route ueber das PPP-Interface, DNS per usepeerdns)
            optional (wifi_share=True) stellt der client diese Verbindung per WLAN-Hotspot weiter bereit
            (NetworkManager, ipv4.method shared: DHCP, DNS-Weiterleitung und NAT fuer die WLAN-Geraete)

    Config (runners):
        network_serial: nxrunner.network.serial:NetworkOverSerial(role="edge", device="/dev/ttyUSB0", baud=115200)
        network_serial: nxrunner.network.serial:NetworkOverSerial(role="client", device="/dev/ttyAMA0", autostart=True)
        network_serial: nxrunner.network.serial:NetworkOverSerial(role="client", wifi_share=True, wifi_ssid="nxserial",
                            wifi_password="geheim123", wifi_ifname="wlan0")
        serial_setup: nxrunner.network.serial:SerialSetup

    Job:
        {"type": "network_serial", "op": "start"}      Verbindung aufbauen (pppd starten, beim edge NAT einrichten)
        {"type": "network_serial", "op": "stop"}       Verbindung trennen und den Serial-Port freigeben
        {"type": "network_serial", "op": "status"}
        {"type": "network_serial", "op": "check"}      Voraussetzungen pruefen
        {"type": "serial_setup", "op": "apply", "role": "client", "device": "/dev/ttyUSB0", "baud": "115200"}

    Voraussetzungen (Linux): Paket ppp (pppd, /dev/ppp), beim edge iptables, fuer den WLAN-Hotspot NetworkManager
    (nmcli) und ein AP-faehiges WLAN-Geraet, Rechte ueber sudo -n (sudo=True) oder Ausfuehrung als root. Auf dem Port darf kein anderer Prozess laufen (serial-getty, ModemManager, ...).
    Beide Seiten muessen dieselbe Baudrate und Flusssteuerung verwenden.
"""
import atexit
import collections
import html
import json
import os
import platform
import re
import shutil
import signal
import socket
import subprocess
import threading
import time
from glob import glob

from nwebclient import runner as r
from nwebclient import base
from nwebclient import web as w
from nwebclient.runner import op

from nxrunner.network.networkmanager import split_terse

ROLES = {'client': "client (nutzt das Internet des edge)", 'edge': "edge (teilt seine Internetverbindung)"}
BAUDS = [9600, 19200, 38400, 57600, 115200, 230400, 460800, 500000, 576000, 921600, 1000000, 1152000, 1500000,
         2000000, 2500000, 3000000, 3500000, 4000000]
FLOWS = {'none': "keine", 'rtscts': "Hardware (RTS/CTS)", 'xonxoff': "Software (XON/XOFF)"}
DEVICE_PATTERNS = ['/dev/serial/by-id/*', '/dev/ttyUSB*', '/dev/ttyACM*', '/dev/ttyAMA*', '/dev/serial[0-9]',
                   '/dev/ttyS[0-9]', '/dev/ttyTHS*', '/dev/ttyGS*']
LINK_SUBNET = ('10.99.0.1', '10.99.0.2')  # edge, client
# Einstellungen, die NetworkOverSerial.configure und SerialSetup kennen
CONFIG_KEYS = ('role', 'device', 'baud', 'flow', 'wifi_share', 'wifi_ssid', 'wifi_password', 'wifi_ifname')
DEFAULT_CONFIG = dict(role='client', device='/dev/ttyUSB0', baud=115200, flow='none', wifi_share=False,
                      wifi_ssid='nxserial', wifi_password='', wifi_ifname='')


def esc(value):
    return html.escape('' if value is None else str(value), quote=True)


def yes(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on', 'ja')


def serial_devices() -> list:
    """ Vorhandene serielle Geraete (by-id zuerst, ohne Duplikate) """
    res = []
    for pattern in DEVICE_PATTERNS:
        for dev in sorted(glob(pattern)):
            if dev not in res:
                res.append(dev)
    return res


def default_route_dev():
    """ Interface der Standardroute (z.B. eth0, wlan0) oder None """
    try:
        out = subprocess.run(['ip', '-4', 'route', 'show', 'default'], stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, universal_newlines=True, timeout=5).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    m = re.search(r'\bdev\s+(\S+)', out)
    return m.group(1) if m else None


class Cmd:
    """ Ausfuehrung von Systembefehlen, optional ueber sudo -n """

    def __init__(self, sudo=True, log=None):
        self.sudo = bool(sudo) and hasattr(os, 'geteuid') and os.geteuid() != 0
        self.log = log

    def argv(self, args, root=True):
        return (['sudo', '-n'] if root and self.sudo else []) + [str(a) for a in args]

    def run(self, *args, root=True, timeout=20):
        """ :return: (ok, ausgabe) """
        cmd = self.argv(args, root)
        try:
            res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True,
                                 timeout=timeout)
            ok, out = res.returncode == 0, res.stdout.strip()
        except FileNotFoundError:
            ok, out = False, f"{cmd[0]} nicht gefunden"
        except subprocess.TimeoutExpired:
            ok, out = False, f"Zeitüberschreitung nach {timeout} s"
        if self.log is not None:
            self.log(f"{self.masked(cmd)}: {'ok' if ok else 'Fehler ' + out}")
        return ok, out

    @staticmethod
    def masked(cmd) -> str:
        """ Kommandozeile fuer das Log, Passwoerter (nach 'password') als *** """
        return ' '.join('***' if i > 0 and cmd[i - 1] == 'password' else a for i, a in enumerate(cmd))


def which(name):
    return shutil.which(name) or next((p for p in (f'/usr/sbin/{name}', f'/sbin/{name}') if os.path.exists(p)), None)


class Check:
    """ Ergebnis einer Voraussetzungs-Pruefung, level: ok, warn, fail """

    def __init__(self, name, level, message, hint=''):
        self.name, self.level, self.message, self.hint = name, level, message, hint

    def to_dict(self):
        return dict(name=self.name, level=self.level, message=self.message, hint=self.hint)


class WifiShare:
    """
    WLAN-Hotspot ueber NetworkManager, der die Internetverbindung (hier ueber PPP) an WLAN-Geraete weitergibt.
    nmcli device wifi hotspot legt ein Profil mit ipv4.method shared an: DHCP + DNS (dnsmasq) und NAT macht NM.
    """

    CON_NAME = 'nxserial-hotspot'

    def __init__(self, cmd: Cmd, ssid='nxserial', password='', ifname='', band=''):
        self.cmd, self.ssid, self.password, self.ifname, self.band = cmd, ssid or 'nxserial', password, ifname, band

    def devices(self) -> list:
        """ WLAN-Geraete als dicts DEVICE, STATE, CONNECTION """
        ok, out = self.cmd.run('nmcli', '-t', '-f', 'DEVICE,TYPE,STATE,CONNECTION', 'device', 'status',
                               root=False, timeout=10)
        if not ok:
            return []
        rows = [dict(zip(('DEVICE', 'TYPE', 'STATE', 'CONNECTION'), split_terse(line)))
                for line in out.splitlines() if line.strip()]
        return [row for row in rows if row.get('TYPE') == 'wifi']

    def device(self):
        """ gewaehltes oder erstes WLAN-Geraet """
        if self.ifname:
            return self.ifname
        devs = self.devices()
        return devs[0]['DEVICE'] if devs else None

    def is_active(self) -> bool:
        ok, out = self.cmd.run('nmcli', '-t', '-f', 'NAME', 'connection', 'show', '--active', root=False, timeout=10)
        return ok and self.CON_NAME in [split_terse(line)[0] for line in out.splitlines()]

    def start(self):
        """ :return: (ok, nachricht) """
        dev = self.device()
        if not dev:
            return False, "Kein WLAN-Gerät gefunden"
        args = ['nmcli', 'device', 'wifi', 'hotspot', 'ifname', dev, 'con-name', self.CON_NAME, 'ssid', self.ssid]
        args += (['password', self.password] if self.password else []) + (['band', self.band] if self.band else [])
        ok, out = self.cmd.run(*args, timeout=60)
        if ok:
            # nur bei Bedarf aktiv (startet mit dem Runner, nicht beim Booten)
            self.cmd.run('nmcli', 'connection', 'modify', self.CON_NAME, 'connection.autoconnect', 'no')
            return True, f"WLAN-Hotspot {self.ssid} auf {dev}"
        return False, "WLAN-Hotspot: " + out.replace(self.password or '\0', '***')

    def stop(self):
        if self.is_active():
            return self.cmd.run('nmcli', 'connection', 'down', 'id', self.CON_NAME, timeout=30)
        return True, "nicht aktiv"

    def checks(self) -> list:
        return [c for c in (self.check_nmcli(), self.check_device(), self.check_password()) if c is not None]

    def check_nmcli(self):
        if not which('nmcli'):
            return Check("NetworkManager", 'fail', "nmcli nicht gefunden", "sudo apt install network-manager")
        ok, out = self.cmd.run('nmcli', '-t', '-f', 'RUNNING', 'general', root=False, timeout=10)
        if ok and out.strip() == 'running':
            return Check("NetworkManager", 'ok', "läuft")
        return Check("NetworkManager", 'fail', "läuft nicht: " + out, "sudo systemctl enable --now NetworkManager")

    def check_device(self):
        if not which('nmcli'):
            return None
        devs = {d['DEVICE']: d for d in self.devices()}
        dev = self.ifname or next(iter(devs), None)
        if dev is None or dev not in devs:
            return Check("WLAN-Gerät", 'fail', f"{dev or 'kein WLAN-Gerät'} nicht gefunden",
                         "Vorhanden: " + (', '.join(devs) or "keine") + " (Gerät muss von NetworkManager verwaltet werden)")
        con = devs[dev].get('CONNECTION', '')
        if con and con not in ('--', self.CON_NAME):
            return Check("WLAN-Gerät", 'warn', f"{dev} ist mit {con} verbunden",
                         "Die WLAN-Verbindung wird durch den Hotspot ersetzt")
        return Check("WLAN-Gerät", 'ok', dev)

    def check_password(self):
        if not self.password:
            return Check("WLAN-Passwort", 'warn', "leer, NetworkManager erzeugt ein zufälliges",
                         "Passwort mit mindestens 8 Zeichen angeben")
        if len(self.password) < 8:
            return Check("WLAN-Passwort", 'fail', "zu kurz", "WPA2 braucht mindestens 8 Zeichen")
        return Check("WLAN-Passwort", 'ok', f"gesetzt, SSID {self.ssid}")


class SerialNetChecks:
    """ Prueft die Voraussetzungen fuer NetworkOverSerial """

    def __init__(self, role, device, baud, sudo=True, wan=None, cmd: Cmd = None, wifi: WifiShare = None):
        self.role, self.device, self.baud, self.wan = role, device, baud, wan
        self.cmd = cmd or Cmd(sudo)
        self.sudo = sudo
        self.wifi = wifi

    @staticmethod
    def from_config(cfg: dict, cmd: Cmd, wan=None) -> 'SerialNetChecks':
        wifi = None
        if cfg.get('role') == 'client' and yes(cfg.get('wifi_share', False)):
            wifi = WifiShare(cmd, cfg.get('wifi_ssid'), cfg.get('wifi_password'), cfg.get('wifi_ifname'))
        return SerialNetChecks(cfg.get('role'), cfg.get('device'), cfg.get('baud'), wan=wan, cmd=cmd, wifi=wifi)

    def run(self) -> list:
        checks = [self.check_os(), self.check_role(), self.check_baud(), self.check_pppd(), self.check_dev_ppp(),
                  self.check_sudo(), self.check_device(), self.check_device_free(), self.check_getty(),
                  self.check_modemmanager()]
        if self.role == 'edge':
            checks += [self.check_iptables(), self.check_wan(), self.check_internet()]
        else:
            checks.append(self.check_client_route())
        if self.wifi is not None:
            checks += self.wifi.checks()
        return [c for c in checks if c is not None]

    @staticmethod
    def ok(checks) -> bool:
        return all(c.level != 'fail' for c in checks)

    def check_os(self):
        if platform.system() == 'Linux':
            return Check("Betriebssystem", 'ok', "Linux")
        return Check("Betriebssystem", 'fail', platform.system(), "PPP über Serial wird nur unter Linux unterstützt")

    def check_role(self):
        if self.role in ROLES:
            return Check("Rolle", 'ok', self.role)
        return Check("Rolle", 'fail', f"Unbekannt: {self.role}", "client oder edge")

    def check_baud(self):
        try:
            baud = int(self.baud)
        except (TypeError, ValueError):
            return Check("Baudrate", 'fail', f"Keine Zahl: {self.baud}")
        if baud in BAUDS:
            return Check("Baudrate", 'ok', str(baud), "Beide Seiten müssen dieselbe Baudrate nutzen")
        return Check("Baudrate", 'warn', f"{baud} ist keine Standard-Baudrate", "z.B. 115200 oder 921600")

    def check_pppd(self):
        path = which('pppd')
        if path:
            return Check("pppd", 'ok', path)
        return Check("pppd", 'fail', "nicht installiert", "sudo apt install ppp")

    def check_dev_ppp(self):
        if os.path.exists('/dev/ppp'):
            return Check("/dev/ppp", 'ok', "Kernel-Unterstützung vorhanden")
        return Check("/dev/ppp", 'fail', "fehlt", "sudo modprobe ppp_generic (bzw. Paket ppp installieren)")

    def check_sudo(self):
        if not self.cmd.sudo:
            is_root = hasattr(os, 'geteuid') and os.geteuid() == 0
            return Check("Rechte", 'ok' if is_root else 'warn', "root" if is_root else "ohne sudo",
                         "" if is_root else "pppd/iptables brauchen root-Rechte, sudo=True verwenden")
        ok, out = self.cmd.run('true', timeout=5)
        if ok:
            return Check("Rechte", 'ok', "sudo -n funktioniert")
        return Check("Rechte", 'fail', "sudo -n nicht möglich: " + out,
                     "NOPASSWD in /etc/sudoers.d/ für pppd, kill, ip, iptables, sysctl, nmcli eintragen")

    def check_device(self):
        if not self.device:
            return Check("Serial-Gerät", 'fail', "kein Gerät angegeben", ', '.join(serial_devices()))
        if not os.path.exists(self.device):
            return Check("Serial-Gerät", 'fail', f"{self.device} existiert nicht",
                         "Vorhanden: " + (', '.join(serial_devices()) or "keine"))
        real = os.path.realpath(self.device)
        rw = os.access(real, os.R_OK | os.W_OK)
        return Check("Serial-Gerät", 'ok' if rw or self.cmd.sudo else 'warn',
                     real + ("" if rw else " (kein Lese-/Schreibzugriff für diesen Benutzer)"),
                     "" if rw else "Benutzer zur Gruppe dialout hinzufügen oder sudo verwenden")

    def check_device_free(self):
        if not self.device or not os.path.exists(self.device):
            return None
        users = device_users(self.device, self.cmd)
        if not users:
            return Check("Port frei", 'ok', "kein Prozess nutzt den Port")
        return Check("Port frei", 'warn', "belegt von: " + ', '.join(users),
                     "Läuft dort schon pppd dieses Runners, ist das ok, sonst den Prozess beenden")

    def check_getty(self):
        if not self.device or not which('systemctl'):
            return None
        name = os.path.basename(os.path.realpath(self.device))
        ok, out = self.cmd.run('systemctl', 'is-active', f'serial-getty@{name}.service', root=False, timeout=5)
        if out.strip() == 'active':
            return Check("Login-Konsole", 'fail', f"serial-getty@{name} läuft auf dem Port",
                         f"sudo systemctl disable --now serial-getty@{name} (und console= aus cmdline.txt entfernen)")
        return Check("Login-Konsole", 'ok', "keine serial-getty auf dem Port")

    def check_modemmanager(self):
        if not which('systemctl'):
            return None
        ok, out = self.cmd.run('systemctl', 'is-active', 'ModemManager.service', root=False, timeout=5)
        if out.strip() == 'active':
            return Check("ModemManager", 'warn', "aktiv, kann den Port beim Anstecken belegen",
                         "sudo systemctl disable --now ModemManager oder udev-Regel ID_MM_DEVICE_IGNORE=1")
        return Check("ModemManager", 'ok', "nicht aktiv")

    def check_iptables(self):
        path = which('iptables')
        if path:
            return Check("iptables", 'ok', path)
        return Check("iptables", 'fail', "nicht installiert", "sudo apt install iptables")

    def check_wan(self):
        wan = self.wan or default_route_dev()
        if wan:
            return Check("Internet-Interface", 'ok', wan + ("" if self.wan else " (Standardroute)"))
        return Check("Internet-Interface", 'fail', "keine Standardroute gefunden",
                     "Der edge braucht eine Internetverbindung oder wan='eth0' angeben")

    def check_internet(self):
        try:
            with socket.create_connection(('1.1.1.1', 53), timeout=3):
                return Check("Internet", 'ok', "1.1.1.1 erreichbar")
        except OSError as e:
            return Check("Internet", 'warn', f"1.1.1.1 nicht erreichbar: {e}")

    def check_client_route(self):
        dev = default_route_dev()
        if dev and not dev.startswith('ppp'):
            return Check("Standardroute", 'warn', f"bereits über {dev}",
                         "Die Route über PPP wird mit kleinerer Metrik zusätzlich gesetzt")
        return Check("Standardroute", 'ok', dev or "keine, wird über PPP gesetzt")


def device_users(device, cmd: Cmd = None) -> list:
    """ Prozesse, die das Geraet geoeffnet haben, als 'pid (name)' """
    real = os.path.realpath(device)
    res = []
    for fd_dir in glob('/proc/[0-9]*/fd'):
        try:
            if any(os.path.realpath(os.path.join(fd_dir, fd)) == real for fd in os.listdir(fd_dir)):
                pid = fd_dir.split('/')[2]
                res.append(f"{pid} ({_proc_name(pid)})")
        except OSError:
            continue  # fremde Prozesse ohne Rechte
    if not res and cmd is not None and cmd.sudo and which('fuser'):
        ok, out = cmd.run('fuser', real, timeout=5)
        res = [f"{pid} ({_proc_name(pid)})" for pid in re.findall(r'\d+', out.split(':', 1)[-1])] if ok else []
    return res


def _proc_name(pid):
    try:
        with open(f'/proc/{pid}/comm') as f:
            return f.read().strip()
    except OSError:
        return '?'


class NetworkOverSerial(r.BaseJobExecutor):
    """
    Netzwerkverbindung über einen Serial-Port mit PPP. Rolle edge teilt das Internet (NAT), client nutzt es.
    """

    LOG_LINES = 200

    def __init__(self, type='network_serial', role='client', device='/dev/ttyUSB0', baud=115200, flow='none',
                 edge_ip=LINK_SUBNET[0], client_ip=LINK_SUBNET[1], unit=9, wan=None, dns='1.1.1.1',
                 default_route=True, route_metric=10, sudo=True, autostart=False, mtu=1500, wifi_share=False,
                 wifi_ssid='nxserial', wifi_password='', wifi_ifname='', wifi_band=''):
        super().__init__(type)
        self.role, self.device, self.baud, self.flow = role, device, int(baud), flow
        self.edge_ip, self.client_ip, self.unit = edge_ip, client_ip, int(unit)
        self.wan, self.dns, self.mtu = wan, dns, int(mtu)
        self.default_route, self.route_metric = yes(default_route), int(route_metric)
        self.wifi_share, self.wifi_ssid, self.wifi_password = yes(wifi_share), wifi_ssid, wifi_password or ''
        self.wifi_ifname, self.wifi_band = wifi_ifname or '', wifi_band or ''
        self.wifi_state = ''  # Ergebnis des letzten Hotspot-Starts
        self.cmd = Cmd(sudo, log=self.info)
        self.proc = None
        self.lock = threading.RLock()
        self.log = collections.deque(maxlen=self.LOG_LINES)
        self.nat_wan = None  # Interface, fuer das NAT-Regeln gesetzt wurden
        self.forward_before = None  # ip_forward vor dem Start (edge)
        self.last_error = ''
        self.define_vars('role', 'device', 'baud', 'flow', 'wifi_share', 'wifi_ssid', 'last_error')
        atexit.register(self.release_on_exit)
        if yes(autostart):
            threading.Timer(2, self.start).start()

    # ------------------------------------------------------------------
    # Eigenschaften
    # ------------------------------------------------------------------

    @property
    def ifname(self):
        return f'ppp{self.unit}'

    @property
    def linkname(self):
        return f'nxserial{self.unit}'

    @property
    def local_ip(self):
        return self.edge_ip if self.role == 'edge' else self.client_ip

    @property
    def remote_ip(self):
        return self.client_ip if self.role == 'edge' else self.edge_ip

    @property
    def wifi(self):
        """ WLAN-Hotspot des client oder None """
        if self.role != 'client' or not self.wifi_share:
            return None
        return WifiShare(self.cmd, self.wifi_ssid, self.wifi_password, self.wifi_ifname, self.wifi_band)

    def checks(self):
        return SerialNetChecks(self.role, self.device, self.baud, wan=self.wan, cmd=self.cmd, wifi=self.wifi)

    def config(self) -> dict:
        return {key: getattr(self, key) for key in CONFIG_KEYS}

    # ------------------------------------------------------------------
    # pppd
    # ------------------------------------------------------------------

    def ppp_args(self) -> list:
        """ Kommandozeile fuer pppd (nodetach: der Runner haelt den Prozess) """
        args = [which('pppd') or 'pppd', self.device, str(self.baud), f'{self.local_ip}:{self.remote_ip}',
                'nodetach', 'noauth', 'local', 'lock', 'persist', 'maxfail', '0', 'holdoff', '3',
                'lcp-echo-interval', '5', 'lcp-echo-failure', '4', 'unit', str(self.unit),
                'linkname', self.linkname, 'mtu', str(self.mtu), 'mru', str(self.mtu),
                'nodefaultroute']
        args += {'rtscts': ['crtscts'], 'xonxoff': ['xonxoff']}.get(self.flow, ['nocrtscts'])
        if self.role == 'edge':
            args += ['ms-dns', self.dns] if self.dns else []
        else:
            args += ['usepeerdns']
        return args

    def pidfile_pid(self):
        """ PID eines laufenden pppd dieses Links (auch nach Neustart des Runners) """
        for path in (f'/run/ppp-{self.linkname}.pid', f'/var/run/ppp-{self.linkname}.pid'):
            try:
                with open(path) as f:
                    pid = int(f.readline().strip())
                if os.path.exists(f'/proc/{pid}'):
                    return pid
            except (OSError, ValueError):
                continue
        return None

    def is_running(self) -> bool:
        if self.proc is not None and self.proc.poll() is None:
            return True
        return self.pidfile_pid() is not None

    def is_up(self) -> bool:
        return os.path.exists(f'/sys/class/net/{self.ifname}')

    def read_output(self, proc):
        for line in proc.stdout:
            self.log.append(time.strftime('%H:%M:%S ') + line.rstrip())
        self.log.append(time.strftime('%H:%M:%S ') + f"pppd beendet (Code {proc.wait()})")

    def spawn_pppd(self):
        cmd = self.cmd.argv(self.ppp_args())
        self.log.append(time.strftime('%H:%M:%S ') + ' '.join(cmd))
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     universal_newlines=True, start_new_session=True)
        threading.Thread(target=self.read_output, args=(self.proc,), daemon=True).start()
        time.sleep(1)
        if self.proc.poll() is not None:
            raise RuntimeError("pppd beendet sich sofort: " + ' | '.join(list(self.log)[-5:]))

    # ------------------------------------------------------------------
    # edge: Internet teilen
    # ------------------------------------------------------------------

    def nat_rules(self, wan) -> list:
        src = f'{self.client_ip}/32'
        return [['-t', 'nat', '{}', 'POSTROUTING', '-s', src, '-o', wan, '-j', 'MASQUERADE'],
                ['{}', 'FORWARD', '-i', self.ifname, '-o', wan, '-s', src, '-j', 'ACCEPT'],
                ['{}', 'FORWARD', '-i', wan, '-o', self.ifname, '-d', src, '-m', 'conntrack',
                 '--ctstate', 'RELATED,ESTABLISHED', '-j', 'ACCEPT']]

    def iptables(self, rule, action):
        return self.cmd.run('iptables', '-w', *[action if a == '{}' else a for a in rule])

    def enable_sharing(self):
        wan = self.wan or default_route_dev()
        if not wan:
            raise RuntimeError("Keine Internetverbindung (Standardroute) zum Teilen gefunden")
        ok, out = self.cmd.run('sysctl', '-n', 'net.ipv4.ip_forward', root=False)
        self.forward_before = out.strip() if ok else None
        ok, out = self.cmd.run('sysctl', '-w', 'net.ipv4.ip_forward=1')
        if not ok:
            raise RuntimeError("IP-Forwarding nicht aktivierbar: " + out)
        for rule in self.nat_rules(wan):
            if not self.iptables(rule, '-C')[0]:  # nur einmal eintragen
                ok, out = self.iptables(rule, '-A')
                if not ok:
                    raise RuntimeError("iptables: " + out)
        self.nat_wan = wan

    def disable_sharing(self):
        if self.nat_wan:
            for rule in self.nat_rules(self.nat_wan):
                while self.iptables(rule, '-C')[0] and self.iptables(rule, '-D')[0]:
                    pass
            self.nat_wan = None
        if self.forward_before == '0':
            self.cmd.run('sysctl', '-w', 'net.ipv4.ip_forward=0')
        self.forward_before = None

    # ------------------------------------------------------------------
    # client: Route
    # ------------------------------------------------------------------

    def wait_up(self, timeout=30) -> bool:
        end = time.time() + timeout
        while time.time() < end:
            if self.is_up() and self.local_ip in self.ip_addr():
                return True
            if not self.is_running():
                return False
            time.sleep(0.5)
        return False

    def route_watcher(self):
        """ Setzt die Default-Route, sobald das Interface oben ist (auch nach Reconnect durch persist) """
        while self.proc is not None and self.proc.poll() is None:
            if self.wait_up(timeout=10):
                self.cmd.run('ip', 'route', 'replace', 'default', 'dev', self.ifname, 'metric', self.route_metric)
                while self.is_up() and self.proc is not None and self.proc.poll() is None:
                    time.sleep(3)
            time.sleep(1)

    def ip_addr(self) -> str:
        ok, out = self.cmd.run('ip', '-4', '-o', 'addr', 'show', 'dev', self.ifname, root=False, timeout=5)
        return out if ok else ''

    # ------------------------------------------------------------------
    # Start / Stop
    # ------------------------------------------------------------------

    def start(self):
        """ :return: (ok, nachricht) """
        with self.lock:
            if self.is_running():
                return True, "läuft bereits"
            checks = self.checks().run()
            failed = [c for c in checks if c.level == 'fail']
            if failed:
                self.last_error = '; '.join(f"{c.name}: {c.message}" for c in failed)
                return False, "Voraussetzungen fehlen: " + self.last_error
            try:
                if self.role == 'edge':
                    self.enable_sharing()
                self.spawn_pppd()
            except Exception as e:
                self.last_error = str(e)
                self.stop()
                return False, str(e)
            if self.role == 'client' and self.default_route:
                threading.Thread(target=self.route_watcher, daemon=True).start()
            self.last_error = ''
            msg = f"pppd gestartet ({self.role}, {self.device} @ {self.baud})"
            return True, msg + self.start_wifi()

    def start_wifi(self) -> str:
        """ Startet den WLAN-Hotspot (client), ein Fehler bricht die Serial-Verbindung nicht ab """
        if self.wifi is None:
            return ''
        ok, self.wifi_state = self.wifi.start()
        if not ok:
            self.last_error = self.wifi_state
        return ', ' + self.wifi_state

    def stop_wifi(self):
        if which('nmcli'):
            WifiShare(self.cmd).stop()  # auch, wenn wifi_share inzwischen abgeschaltet wurde
        self.wifi_state = ''

    def stop(self, timeout=10):
        """ Beendet Hotspot und pppd, entfernt NAT-Regeln und gibt den Serial-Port frei. :return: (ok, nachricht) """
        with self.lock:
            if self.role == 'client':
                self.stop_wifi()
            self.terminate_pppd(timeout)
            if self.role == 'edge' or self.nat_wan:
                self.disable_sharing()
            users = device_users(self.device, self.cmd) if self.device and os.path.exists(self.device) else []
            if users:
                return False, "Port weiterhin belegt von: " + ', '.join(users)
            return True, f"{self.device} freigegeben"

    def terminate_pppd(self, timeout):
        proc, self.proc = self.proc, None
        pid = self.pidfile_pid()
        if pid is not None:
            self.cmd.run('kill', '-TERM', pid)
        elif proc is not None and proc.poll() is None:
            self.signal_group(proc, signal.SIGTERM)
        end = time.time() + timeout
        while time.time() < end and (self.pidfile_pid() is not None or (proc is not None and proc.poll() is None)):
            time.sleep(0.3)
        pid = self.pidfile_pid()
        if pid is not None:
            self.cmd.run('kill', '-KILL', pid)
        if proc is not None and proc.poll() is None:
            self.signal_group(proc, signal.SIGKILL)

    def signal_group(self, proc, sig):
        try:
            os.killpg(proc.pid, sig)
        except (OSError, AttributeError):
            self.cmd.run('kill', '-' + str(int(sig)), proc.pid)

    def release_on_exit(self):
        try:
            if self.proc is not None and self.proc.poll() is None:
                self.stop(timeout=5)
        except Exception:
            pass

    def configure(self, **cfg):
        """ Aendert Einstellungen aus CONFIG_KEYS (leere Werte bleiben unveraendert), nur wenn gestoppt """
        if self.is_running():
            raise RuntimeError("Verbindung läuft, zuerst stoppen (Port freigeben)")
        cfg = {k: v for k, v in cfg.items() if k in CONFIG_KEYS and v not in (None, '')}
        if 'role' in cfg and cfg['role'] not in ROLES:
            raise ValueError("role muss client oder edge sein")
        if 'flow' in cfg and cfg['flow'] not in FLOWS:
            raise ValueError("flow muss " + ', '.join(FLOWS) + " sein")
        if len(cfg.get('wifi_password', '12345678')) < 8:
            raise ValueError("WLAN-Passwort braucht mindestens 8 Zeichen")
        for key, value in cfg.items():
            if key == 'baud':
                value = int(value)
            elif key == 'wifi_share':
                value = yes(value)
            setattr(self, key, value)

    # ------------------------------------------------------------------
    # Operationen
    # ------------------------------------------------------------------

    @op("Verbindung über den Serial-Port aufbauen", result={'success': 'bool', 'message': 'str'}, name='start')
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

    @op("Status der Verbindung", success={'status': 'dict'}, fail={}, name='status')
    def op_status(self):
        return self.success(status=self.get_status())

    @op("Voraussetzungen prüfen", success={'checks': 'list', 'ok': 'bool'}, fail={}, name='check')
    def op_check(self):
        checks = self.checks().run()
        return self.success(ok=SerialNetChecks.ok(checks), checks=[c.to_dict() for c in checks])

    @op("Einstellungen ändern (nur gestoppt)", result={'success': 'bool', 'message': 'str'}, name='configure')
    def op_configure(self, role: str = '', device: str = '', baud: str = '', flow: str = '', wifi_share: str = '',
                     wifi_ssid: str = '', wifi_password: str = '', wifi_ifname: str = ''):
        try:
            self.configure(role=role, device=device, baud=baud, flow=flow, wifi_share=wifi_share,
                           wifi_ssid=wifi_ssid, wifi_password=wifi_password, wifi_ifname=wifi_ifname)
        except (RuntimeError, ValueError) as e:
            return self.fail(str(e))
        return self.success("Gespeichert", ui_reload=True)

    @op("WLAN-Hotspot des client starten (Verbindung muss laufen)", result={'success': 'bool', 'message': 'str'})
    def wifi_start(self):
        if self.wifi is None:
            return self.fail("WLAN-Freigabe ist nicht aktiviert (nur Rolle client, wifi_share)")
        ok, self.wifi_state = self.wifi.start()
        return self.success(self.wifi_state, ui_reload=True) if ok else self.fail(self.wifi_state)

    @op("WLAN-Hotspot stoppen, die Serial-Verbindung bleibt", result={'success': 'bool', 'message': 'str'})
    def wifi_stop(self):
        self.stop_wifi()
        return self.success("WLAN-Hotspot gestoppt", ui_reload=True)

    def get_status(self) -> dict:
        wifi = self.wifi
        return dict(role=self.role, device=self.device, baud=self.baud, flow=self.flow, running=self.is_running(),
                    up=self.is_up(), ifname=self.ifname, local_ip=self.local_ip, remote_ip=self.remote_ip,
                    addr=self.ip_addr() if self.is_up() else '', nat_wan=self.nat_wan or '',
                    wifi_share=self.wifi_share, wifi_active=wifi is not None and wifi.is_active(),
                    wifi_ssid=self.wifi_ssid if wifi is not None else '', last_error=self.last_error)

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def btn(self, title, op_name, hint, **params):
        return self.action_btn(dict(type=self.type, op=op_name, title=title, **params), hint=hint)

    def part_index(self, p: base.Page, params={}):
        self.part_state(p)
        self.part_settings(p)
        with p.section(h="Voraussetzungen"):
            part_checks(p, self.checks().run())
        with p.expandable("pppd-Ausgabe", expanded=False):
            p.pre(esc('\n'.join(self.log) or "Noch keine Ausgabe"))

    def part_state(self, p: base.Page):
        status = self.get_status()
        with p.section(h="Netzwerk über Serial"):
            state = "verbunden" if status['up'] else ("verbinde ..." if status['running'] else "gestoppt")
            p.prop("Zustand", state)
            for key, title in (('role', "Rolle"), ('device', "Gerät"), ('baud', "Baudrate"), ('flow', "Fluss"),
                               ('ifname', "Interface"), ('local_ip', "Lokale IP"), ('remote_ip', "Gegenstelle"),
                               ('nat_wan', "Geteilt über"), ('last_error', "Letzter Fehler")):
                if status[key] not in ('', None):
                    p.prop(title, esc(status[key]))
            p.div(self.btn("Verbinden", 'start', "Startet pppd auf dem Serial-Port (edge: teilt das Internet)")
                  + self.btn("Trennen", 'stop', "Beendet pppd, entfernt NAT-Regeln und gibt den Serial-Port frei")
                  + self.btn("Neu verbinden", 'restart', "Trennt und baut die Verbindung neu auf"))
            self.part_wifi(p, status)

    def part_wifi(self, p: base.Page, status):
        if self.wifi is None:
            return
        p.prop("WLAN-Hotspot", esc(f"{status['wifi_ssid']} ({'aktiv' if status['wifi_active'] else 'aus'})"))
        if self.wifi_state:
            p.prop("WLAN-Start", esc(self.wifi_state))
        p.div(self.btn("Hotspot starten", 'wifi_start', "Gibt die Internetverbindung per WLAN an Geräte weiter")
              + self.btn("Hotspot stoppen", 'wifi_stop', "Schaltet den WLAN-Hotspot ab, die Serial-Verbindung bleibt"))

    def part_settings(self, p: base.Page):
        if self.is_running():
            return
        with p.section(h="Einstellungen"):
            part_form(p, 'nos_', self.config())
            p.div(self.action_btn_parametric("Übernehmen", form_job(self.type, 'configure', 'nos_'),
                                             hint="Speichert Rolle, Gerät, Baudrate, Flusssteuerung und WLAN"))


# ----------------------------------------------------------------------
# gemeinsame UI
# ----------------------------------------------------------------------

LEVEL_TITLE = {'ok': "OK", 'warn': "Hinweis", 'fail': "Fehlt"}


def part_checks(p: base.Page, checks: list):
    rows = [dict(c.to_dict(), state=LEVEL_TITLE.get(c.level, c.level)) for c in checks]
    p.grid(rows, [w.Grid.col('state', "Status"), w.Grid.col('name', "Prüfung"), w.Grid.col('message', "Ergebnis"),
                  w.Grid.col('hint', "Hinweis")], empty="Keine Prüfungen")
    if SerialNetChecks.ok(checks):
        p(w.alert_success("Alle Voraussetzungen erfüllt"))
    else:
        p(w.alert_error("Es fehlen Voraussetzungen"))


def form_job(type_name, op_name, prefix, **extra) -> dict:
    """ Job, der die Werte aller Formularfelder (part_form) mitsendet """
    return dict(type=type_name, op=op_name, **{key: '#' + prefix + key for key in CONFIG_KEYS}, **extra)


def part_form(p: base.Page, prefix, cfg: dict):
    p.form_select('role', "Rolle", ROLES, value=cfg['role'], id=prefix + 'role',
                  hint="edge teilt seine Internetverbindung, client nutzt sie")
    devices = serial_devices()
    if cfg['device'] and cfg['device'] not in devices:
        devices.insert(0, cfg['device'])
    p.form_select('device', "Serial-Gerät", devices, value=cfg['device'], id=prefix + 'device',
                  hint="Serieller Port, /dev/serial/by-id/... bleibt auch nach Umstecken gleich")
    p.form_select('baud', "Baudrate", BAUDS, value=cfg['baud'], id=prefix + 'baud',
                  hint="Muss auf beiden Seiten gleich sein")
    p.form_select('flow', "Flusssteuerung", FLOWS, value=cfg['flow'], id=prefix + 'flow',
                  hint="RTS/CTS nur, wenn die Leitungen verbunden sind; auf beiden Seiten gleich")
    part_form_wifi(p, prefix, cfg)


def part_form_wifi(p: base.Page, prefix, cfg: dict):
    p.form_select('wifi_share', "WLAN-Freigabe", {'no': "nein", 'yes': "ja (nur client)"},
                  value='yes' if yes(cfg['wifi_share']) else 'no', id=prefix + 'wifi_share',
                  hint="Der client gibt die Internetverbindung des edge per WLAN-Hotspot an Geräte weiter")
    p.form_input('wifi_ssid', "WLAN-Name", hint="SSID des Hotspots", id=prefix + 'wifi_ssid',
                 value=esc(cfg['wifi_ssid']))
    p.form_input('wifi_password', "WLAN-Passwort", type='password', id=prefix + 'wifi_password', value='',
                 hint="Mindestens 8 Zeichen, leer = unverändert" + (" (gesetzt)" if cfg['wifi_password'] else ""))
    ifnames = {'': "automatisch"}
    if yes(cfg['wifi_share']) or cfg['wifi_ifname']:
        ifnames.update({d['DEVICE']: d['DEVICE'] for d in WifiShare(Cmd(False)).devices()})
    if cfg['wifi_ifname']:
        ifnames.setdefault(cfg['wifi_ifname'], cfg['wifi_ifname'])
    p.form_select('wifi_ifname', "WLAN-Gerät", ifnames, value=cfg['wifi_ifname'], id=prefix + 'wifi_ifname',
                  hint="WLAN-Gerät für den Hotspot (muss den AP-Modus können)")


class SerialSetup(r.BaseJobExecutor):
    """
    Richtet NetworkOverSerial ein: Rolle, Baudrate und Serial-Gerät wählen, Voraussetzungen prüfen,
    fehlende Pakete installieren und den Runner im Dispatcher anlegen.
    """

    def __init__(self, type='serial_setup', runner='network_serial', sudo=True):
        super().__init__(type)
        self.runner_type = runner
        self.cmd = Cmd(sudo, log=self.info)
        self.last = dict(DEFAULT_CONFIG, device=(serial_devices() or ['/dev/ttyUSB0'])[0])
        self.last_output = ''

    def params(self, **values) -> dict:
        """ Neue Einstellungen: leere Werte uebernehmen die zuletzt verwendeten """
        cfg = dict(self.last)
        cfg.update({k: v for k, v in values.items() if k in CONFIG_KEYS and v not in (None, '')})
        cfg['baud'], cfg['wifi_share'] = int(cfg['baud']), yes(cfg['wifi_share'])
        return cfg

    def find_runner(self):
        disp = self.getParentClass(r.LazyDispatcher)
        if disp is None:
            return None, None
        return disp, disp.get_runner(self.runner_type)

    @op("Voraussetzungen für NetworkOverSerial prüfen", success={'checks': 'list', 'ok': 'bool'}, fail={})
    def check(self, role: str = '', device: str = '', baud: str = '', flow: str = '', wifi_share: str = '',
              wifi_ssid: str = '', wifi_password: str = '', wifi_ifname: str = ''):
        self.last = self.params(role=role, device=device, baud=baud, flow=flow, wifi_share=wifi_share,
                                wifi_ssid=wifi_ssid, wifi_password=wifi_password, wifi_ifname=wifi_ifname)
        checks = SerialNetChecks.from_config(self.last, self.cmd).run()
        return self.success(ok=SerialNetChecks.ok(checks), checks=[c.to_dict() for c in checks], ui_reload=True)

    @op("NetworkOverSerial anlegen bzw. konfigurieren", result={'success': 'bool', 'message': 'str'})
    def apply(self, role: str = '', device: str = '', baud: str = '', flow: str = '', wifi_share: str = '',
              wifi_ssid: str = '', wifi_password: str = '', wifi_ifname: str = '', start: str = 'no'):
        self.last = cfg = self.params(role=role, device=device, baud=baud, flow=flow, wifi_share=wifi_share,
                                      wifi_ssid=wifi_ssid, wifi_password=wifi_password, wifi_ifname=wifi_ifname)
        if cfg['role'] not in ROLES:
            return self.fail("role muss client oder edge sein")
        disp, runner = self.find_runner()
        try:
            if isinstance(runner, NetworkOverSerial):
                runner.configure(**cfg)
            elif runner is not None:
                return self.fail(f"Runner {self.runner_type} ist kein NetworkOverSerial")
            elif disp is None:
                return self.fail("Kein Dispatcher gefunden, Runner per Config anlegen: " + self.config_line())
            else:
                runner = NetworkOverSerial(self.runner_type, sudo=self.cmd.sudo, **cfg)
                disp.add_runner(runner)
        except (RuntimeError, ValueError) as e:
            return self.fail(str(e))
        self.setting_set('last', {k: v for k, v in cfg.items() if k != 'wifi_password'})  # kein Klartext-Passwort
        if yes(start):
            ok, msg = runner.start()
            return self.success(msg, ui_reload=True) if ok else self.fail(msg)
        return self.success(f"{self.runner_type} eingerichtet ({cfg['role']})", ui_reload=True)

    @op("Serial-Port freigeben (Verbindung des Runners trennen)", result={'success': 'bool', 'message': 'str'})
    def release(self):
        disp, runner = self.find_runner()
        if not isinstance(runner, NetworkOverSerial):
            return self.fail(f"Runner {self.runner_type} nicht vorhanden")
        ok, msg = runner.stop()
        return self.success(msg, ui_reload=True) if ok else self.fail(msg)

    @op("Pakete ppp und iptables installieren (apt)", result={'success': 'bool', 'message': 'str'})
    def install(self):
        if not which('apt-get'):
            return self.fail("apt-get nicht gefunden, ppp und iptables manuell installieren")
        ok, out = self.cmd.run('env', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', 'install', '-y', 'ppp',
                               'iptables', timeout=600)
        self.last_output = out
        if ok:
            self.cmd.run('modprobe', 'ppp_generic')
        return self.success("Installiert", ui_reload=True) if ok else self.fail(out[-500:])

    @op("Login-Konsole (serial-getty) auf dem Port abschalten", result={'success': 'bool', 'message': 'str'})
    def disable_getty(self, device: str = ''):
        name = os.path.basename(os.path.realpath(device or self.last['device']))
        ok, out = self.cmd.run('systemctl', 'disable', '--now', f'serial-getty@{name}.service')
        self.last_output = out
        return self.success(f"serial-getty@{name} abgeschaltet", ui_reload=True) if ok else self.fail(out)

    def config_line(self):
        """ Zeile fuer die Runner-Config, das WLAN-Passwort wird nicht angezeigt """
        c = self.last
        args = f'role="{c["role"]}", device="{c["device"]}", baud={c["baud"]}, flow="{c["flow"]}"'
        if c['role'] == 'client' and c['wifi_share']:
            args += f', wifi_share=True, wifi_ssid="{c["wifi_ssid"]}", wifi_password="***"'
            args += f', wifi_ifname="{c["wifi_ifname"]}"' if c['wifi_ifname'] else ''
        return f'{self.runner_type}: nxrunner.network.serial:NetworkOverSerial({args})'

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def p_btn(self, title, op_name, hint, **extra):
        return self.action_btn_parametric(title, form_job(self.type, op_name, 'ss_', **extra), hint=hint)

    def part_index(self, p: base.Page, params={}):
        self.load_last()
        with p.section(h="Netzwerk über Serial einrichten"):
            part_form(p, 'ss_', self.last)
            p.div(self.p_btn("Prüfen", 'check', "Prüft alle Voraussetzungen für die gewählten Einstellungen")
                  + self.p_btn("Einrichten", 'apply', f"Legt den Runner {self.runner_type} an bzw. konfiguriert ihn")
                  + self.p_btn("Einrichten + Verbinden", 'apply', "Richtet ein und startet die Verbindung",
                               start='yes')
                  + self.action_btn(dict(type=self.type, op='release', title="Port freigeben"),
                                    hint="Trennt die Verbindung und gibt den Serial-Port frei"))
            p.div(self.action_btn(dict(type=self.type, op='install', title="ppp installieren"),
                                  hint="Installiert ppp und iptables per apt-get (sudo)")
                  + self.p_btn("Login-Konsole abschalten", 'disable_getty',
                               "Deaktiviert serial-getty auf dem gewählten Port, damit pppd ihn nutzen kann"))
        with p.section(h="Voraussetzungen"):
            part_checks(p, SerialNetChecks.from_config(self.last, self.cmd).run())
        self.part_runner(p)

    def part_runner(self, p: base.Page):
        with p.section(h="Runner"):
            disp, runner = self.find_runner()
            if isinstance(runner, NetworkOverSerial):
                st = runner.get_status()
                p.prop("Runner", w.a(esc(self.runner_type), runner.link(runner.part_index)))
                p.prop("Zustand", "verbunden" if st['up'] else ("läuft" if st['running'] else "gestoppt"))
                p.prop("Konfiguration", esc(f"{st['role']}, {st['device']} @ {st['baud']}"))
                if st['wifi_ssid']:
                    p.prop("WLAN-Hotspot", esc(f"{st['wifi_ssid']} ({'aktiv' if st['wifi_active'] else 'aus'})"))
            else:
                p.div(f"Runner {esc(self.runner_type)} ist noch nicht angelegt.")
            p.div("Dauerhaft in der Runner-Config:")
            p.pre(esc(self.config_line()))
            if self.last_output:
                p.pre(esc(self.last_output[-2000:]))

    def load_last(self):
        try:
            stored = self.setting('last', None)
            if stored:
                self.last.update(json.loads(stored) if isinstance(stored, str) else stored)
        except Exception as e:
            self.error(f"Einstellungen nicht lesbar: {e}")
