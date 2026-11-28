"""
Linux-Runner (verschoben aus nxbot.linux, dort bleiben Kompatibilitaets-Imports)
"""
import sys
import os.path
import time
from pathlib import Path
import configparser
import re
import subprocess
import shlex
from collections import defaultdict
import html
import json
import threading

from nwebclient.runner import BaseJobExecutor
from nwebclient.runner import ProcessExecutor
from nwebclient import base as b, base
from nwebclient import web as w
from nwebclient import util as u
from nwebclient import runner as r
from nwebclient import nx


class USBDeviceInfo:
    KNOWN = {
        'Bus 001 Device 070: ID 0cf3:9271 Qualcomm Atheros Communications AR9271 802.11n': "Wifi Stick",
        'Bus 001 Device 069: ID 1a86:5722 QinHeng Electronics UsbMonitor': "USB-C Display",
        'Bus 001 Device 071: ID 1a86:7523 QinHeng Electronics CH340 serial converter': "Serial Converter",
        'Bus 001 Device 002: ID 0bda:f179 Realtek Semiconductor Corp. RTL8188FTV 802.11b/g/n 1T1R 2.4G WLAN Adapter': "Wifi Stick",
        'Bus 001 Device 003: ID 1a86:8012 QinHeng Electronics WCH-Link': "CH32 Programmer",
        'Bus 001 Device 001: ID 1e45:8022 Suyin HD Camera': "Webcam",
        'Bus 001 Device 010: ID 534d:0021 MacroSilicon MS210x Video Grabber [EasierCAP]': "Analog Video In (NOT WORKING)"
    }
    pattern = re.compile(r'Bus (\d{3}) Device (\d{3}): ID (\w+):(\w+) (.+)')

    def __init__(self, lsusb_line: str):
        match = self.pattern.match(lsusb_line)
        if not match:
            raise ValueError(f"Line doesn't match lsusb format: {lsusb_line}")
        self.bus, self.device, self.vendor_id, self.product_id, self.description = match.groups()

    def __repr__(self):
        return (f"USBDevice(bus={self.bus}, device={self.device}, "
                f"vendor_id={self.vendor_id}, product_id={self.product_id}, "
                f"description='{self.description}')")

    def is_serial_device(self, device_info: dict[str, str]):
        # ID_USB_VENDOR ID_USB_MODEL_ID
        return device_info.get('ID_USB_VENDOR') == self.vendor_id and device_info.get('ID_USB_MODEL_ID') == self.product_id


class USBDevices(BaseJobExecutor):
    """
    Erkennt USB-Geräte per lsusb und löst Events beim Hinzufügen und Entfernen von Geräten aus.
    """
    def __init__(self):
        super().__init__('usbdevices')
        self.devices = u.List()
        self.define_event('on_add')
        self.define_event('on_remove')

    def scan_devices(self):
        cmd = 'lsusb'
        devices = list(map(USBDeviceInfo, r.ProcessExecutor(cmd).waitForEnd().lines()))
        self.devices.update_list(devices, self.on_add, self.on_remove)

    def on_add(self, elem):
        self.emit_event('on_add', device=elem)
        if 'UsbMonitor' in elem:
            pass # TODO

    def on_remove(self, elem):
        self.emit_event('on_remove', device=elem)


class SerialDevices:
    def list_devices(self) -> list[str]:
        cmd = 'ls -1 /dev/ttyS* /dev/ttyUSB* /dev/ttyA*'
        return r.ProcessExecutor(cmd).waitForEnd().lines()

    def device_info(self, dev) -> dict[str, str]:
        """
            DEVNAME=/dev/ttyUSB0
            E: MAJOR=188
            E: MINOR=0
            E: SUBSYSTEM=tty
            E: USEC_INITIALIZED=26570025794320
            E: ID_BUS=usb
            E: ID_MODEL=USB_Serial
            E: ID_MODEL_ENC=USB\x20Serial
            E: ID_MODEL_ID=7523
            E: ID_SERIAL=1a86_USB_Serial
            E: ID_VENDOR=1a86
            E: ID_VENDOR_ENC=1a86
            E: ID_VENDOR_ID=1a86
            E: ID_REVISION=0264
            E: ID_TYPE=generic
            E: ID_USB_MODEL=USB_Serial
            E: ID_USB_MODEL_ENC=USB\x20Serial
            E: ID_USB_MODEL_ID=7523
            E: ID_USB_SERIAL=1a86_USB_Serial
            E: ID_USB_VENDOR=1a86
            E: ID_USB_VENDOR_ENC=1a86
            E: ID_USB_VENDOR_ID=1a86
            E: ID_USB_REVISION=0264
            E: ID_USB_TYPE=generic
            E: ID_USB_INTERFACES=:ff0102:
            E: ID_USB_INTERFACE_NUM=00
            E: ID_USB_DRIVER=ch341
            E: ID_USB_CLASS_FROM_DATABASE=Vendor Specific Class
            E: ID_VENDOR_FROM_DATABASE=QinHeng Electronics
            E: ID_MODEL_FROM_DATABASE=CH340 serial converter
            E: ID_PATH=platform-fd500000.pcie-pci-0000:01:00.0-usb-0:1.4:1.0
            E: ID_PATH_TAG=platform-fd500000_pcie-pci-0000_01_00_0-usb-0_1_4_1_0
            E: ID_MM_CANDIDATE=1
            E: DEVLINKS=/dev/serial/by-path/platform-fd500000.pcie-pci-0000:01:00.0-usb-0:1.4:1.0-port0 /dev/serial/by-id/usb-1a86_USB_Serial-if00-port0
            E: TAGS=:systemd:
            E: CURRENT_TAGS=:systemd:
        :param dev:
        :return:
        """
        res = {}
        cmd = f'sudo udevadm info --query=all --name={dev}'
        lines = r.ProcessExecutor(cmd).waitForEnd().lines()
        for line in lines:
            if line.startswith('E:'):
                a = line[3:].split('=')
                res[a[0]] = a[1].strip()
        res['DEVNAME'] = dev
        return res

    def lsusb(self) -> list[USBDeviceInfo]:
        cmd = 'lsusb'
        return list(map(USBDeviceInfo, r.ProcessExecutor(cmd).waitForEnd().lines()))

    def list_usb(self) -> dict[str, USBDeviceInfo]:
        res = {}
        usbs = self.lsusb()
        for dev in self.list_devices():
            info = self.device_info(dev)
            for usb in usbs:
                if usb.is_serial_device(info):
                    res[dev] = usb
        return res


class System(BaseJobExecutor):
    """
    Zeigt Informationen zum Linux-System an (Python, installierte Kommandos, Docker, Netzwerk, SSH, PM2, USB, Journal, Fail2Ban).
    """

    MODULES = ['sshconf', 'pm2py', 'psutil']

    starred = True

    command_list = ['php', 'pip', 'node', 'npm', 'pm2', 'nano', 'docker']
    commands = {}

    exec = [
        {'title': 'Uname', 'cmd': 'uname -a'},
        {'title': 'Arch', 'cmd': 'uname -m'},
        {'title': 'Issue', 'cmd': 'cat /etc/issue'},
        {'title': 'PyTorch', 'cmd': 'pip show torch | grep Version'},
        {'title': 'Model', 'cmd': 'cat /sys/firmware/devicetree/base/model'},
        {'title': 'DF', 'cmd': 'df -h | grep "dev/"'}
    ]

    python = None

    def __init__(self, args: u.Args = None):
        super().__init__('linux')
        if args is None:
            args = u.Args()
        self.args = args
        for c in self.command_list:
            self.check_command(c)
        for c in self.exec:
            self.exec_command(c)

        def on_python_end(p):
            self.python = p.text()
        ProcessExecutor('which python', on_end=on_python_end)

    def is_raspberry_pi(self):
        return 'Raspberry' in ProcessExecutor('cat /sys/firmware/devicetree/base/model').waitForEnd().text()

    def check_command(self, command):
        def on_process_end(p):
            self.commands[command] = p.exit_code == 0
        ProcessExecutor('which ' + command, on_end=on_process_end)

    def exec_command(self, c):
        def on_process_end(p):
            c['result'] = p.text()
        ProcessExecutor(c['cmd'], on_end=on_process_end)

    def show_cmd(self, cmd):
        p = ProcessExecutor(cmd['cmd'], start=True)
        lines = p.waitForEnd().lines()
        result = ''
        if len(lines) > 0:
            result = lines[0]
        s = cmd.get('title', 'Unknown') + ": " + result
        if cmd.get('title', 'Unknown') == 'Model' and result.startswith('Raspberr'):
            s += '&nbsp;&nbsp;' + w.a("Rpi", '?type=rpi')
        return s

    def part_index(self, p: b.Page, params={}):
        with p.section(h="System"):
            p.div("Python-Version: " + sys.version)
            p.div("Python Command: " + str(self.python) + "($ python)")
            p.div("Python Executable: " + str(sys.executable) + " (sys.executable)")
            p.div("Hostname: " + nx.get_name())
            from importlib.metadata import version
            p.div("NWebClient: " + str(version('nwebclient')))
        with p.section(h="Commands"):
            for command in self.commands.keys():
                p.div("Command " + command + ": " + str(self.commands.get(command, None)))
            for cmd in self.exec:
                p.div(self.show_cmd(cmd))
            if os.path.exists('/var/run/docker.sock'):
                p.div("Docker Engine running, /var/run/docker.sock exists")
                self.onParentClass(r.LazyDispatcher, lambda d: self.suggest_docker(p, d))
            else:
                p.div("Docker Engine not running, /var/run/docker.sock not exists")
            self.onParentClass(r.LazyDispatcher, lambda d: self.suggest_python(p, d))
            if self.commands['pm2']:
                self.part_pm2(p)

        with p.section(h="Parts"):
            self.part_network(p, params)
            self.part_ssh(p, params)
            self.part_repos(p, params)
            self.part_nweb(p, params)

        with p.section(h="Services"):
            p.ul([
                w.a("USB", self.link(self.part_usb)),
                w.a("Tree", self.link(self.part_tree)),
                w.a("Journal", self.link(self.part_journal)),
                w.a("Fail2Ban", self.link(self.part_fail2ban)),
            ])

    def part_journal(self, p: b.Page, params={}):
        cmd = 'sudo journalctl --since "90min ago"'
        p.pre(ProcessExecutor(cmd).waitForEnd().text())

    def part_usb(self, p: b.Page, params={}):
        p.h1("USB")
        proc = r.ProcessExecutor('lsusb').waitForEnd()
        # Bus 001 Device 002: ID 0bda:f179 Realtek Semiconductor Corp. RTL8188FTV 802.11b/g/n 1T1R 2.4G WLAN Adapter
        # Bus 001 Device 003: ID 1a86:8012 QinHeng Electronics WCH-Link
        # USBDevices
        p.pre(proc.text())

    def part_nweb(self, p: b.Page, params={}):
        nw = getattr(self.getRoot(), 'nweb', None)
        if nw is not None:
            p.div('Internal nweb available')
        else:
            p.div('nweb not available.')

    def part_repos(self, p: b.Page, params={}):
        p.div("Repos")
        repos = self.args.get('repos', {})
        p.ul(list(repos.keys()))

    def part_network(self, p: b.Page, params={}):
        try:
            self.onParentClass(r.LazyDispatcher, lambda d: self.show_network(p, d, params))
        except Exception as e:
            self.error(e)

    def show_network(self, p: b.Page, d: r.LazyDispatcher, params):
        p(self.ui_runner_link('network', 'nxbot.network:NetworkBot()', params))
        if not d.support_type('network'):
            p("No NetworkBot")

    def part_ssh(self, p: b.Page, params={}):
        try:
            from sshconf import read_ssh_config, empty_ssh_config_file
            from os.path import expanduser

            c = read_ssh_config(expanduser("~/.ssh/config"))
            p.prop("hosts", str(c.hosts()))
            # c.host('fog') {'user': 'ubuntu', 'identityfile': '~/.ssh/pri.pri', 'hostname': '10.10.10.229'}
            p("TODO part ssh")

        except Exception as e:
            p.alert_error(e)
            self.error(e)

    def part_pm2(self, p: b.Page, params={}):
        try:
            p.div("PM2")
            # https://pypi.org/project/pm2py/
            from pm2py import PM2
            pm2 = PM2()
            #pm2.start("tests/test_script2.py", ["--name", "erkinbaba", "-f"])
            for process in pm2.list():
                if process.status == "online":
                    p.div("PROCESS ONLINE: " + process.name)
                else:
                    p.div(f"PROCESS {process.status.upper()}: " + process.name)
        except Exception as e:
            self.error(str(e))

    def suggest_python(self, p: b.Page, d: r.LazyDispatcher):
        if d.support_type('python'):
            p.div(w.a("Python System", f'?type=python'))

    def suggest_docker(self, p: b.Page, d: r.LazyDispatcher):
        if d.support_type('python'):
            p.div(w.a("Docker", f'?type=docker'))
        else:
            p.div("TODO suggest_docker")

    @staticmethod
    def bytes_to_mb(value):
        return round(value / 1024 / 1024, 2)

    @staticmethod
    def print_process_tree(pid=1, indent=0):
        import psutil
        result = ''
        try:
            proc = psutil.Process(pid)
            mem = proc.memory_info().rss  # Resident Set Size in Bytes
            result += " " * indent + f"{proc.pid} {proc.name()} - {System.bytes_to_mb(mem)} MB\n"
            for child in proc.children(recursive=False):
                result += System.print_process_tree(child.pid, indent + 4)
        except psutil.NoSuchProcess:
            pass  # Process may have exited
        except psutil.AccessDenied:
            pass  # Might need sudo for some processes
        return result

    def part_tree(self, p: b.Page, params={}):
        p.pre(self.print_process_tree())

    def part_fail2ban(self, p: b.Page, params={}):
        p.pre('sudo fail2ban-client status sshd')
        # fail2ban-client set jail_name banip xx.xx.xx.xx


class _TextBuffer:
    """
    Thread-sicherer Text-Puffer mit fortlaufendem Offset.
    Clients lesen inkrementell mit read(since) und bekommen den naechsten Offset zurueck.
    """

    def __init__(self, max_chars=200000):
        self.max_chars = max_chars
        self._text = ''
        self._base = 0  # Offset des ersten Zeichens in _text
        self._lock = threading.Lock()

    @property
    def end(self) -> int:
        return self._base + len(self._text)

    def append(self, text: str):
        if not text:
            return
        with self._lock:
            self._text += text
            overflow = len(self._text) - self.max_chars
            if overflow > 0:
                self._text = self._text[overflow:]
                self._base += overflow

    def read(self, since: int = 0):
        with self._lock:
            start = max(int(since), self._base)
            return self._text[start - self._base:], self._base + len(self._text)

    def text(self) -> str:
        with self._lock:
            return self._text

    def clear(self):
        with self._lock:
            self._base += len(self._text)
            self._text = ''


class _ConsoleWriter:
    """ File-artiges Objekt fuer invoke/fabric (out_stream/err_stream) """

    ANSI = re.compile(r'\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b[()][0-9A-B]|\r(?!\n)')

    def __init__(self, log):
        """ :param log: fn(text) """
        self.log = log

    def write(self, s):
        self.log(self.ANSI.sub('', s))
        return len(s)

    def flush(self):
        pass

    def isatty(self):
        return False


class SshBot(BaseJobExecutor):
    """
     Richtet ein entferntes Linux-System per SSH ein.

     Nach dem Verbinden (Startseite) gelangt man auf die Verbindungsseite (?type=sshbot&sshbot=connection).
     Dort lassen sich die einzelnen Installationsschritte starten, die Ausgabe erscheint live in der Konsole.
     Zusaetzlich gibt es ein interaktives Terminal (xterm.js, Fallback: einfache Eingabezeile).

     Jobs (alle mit type=sshbot):
       op=connect, host, port, username, password, key_filename   Verbinden (Passwort leer: SSH-Agent/Schluessel)
       op=disconnect                                Verbindung trennen
       op=start, name=<schritt>                     Installationsschritt im Hintergrund starten (siehe OPS)
       op=command, command=<cmd>                    Befehl im Hintergrund ausfuehren
       op=console, since=<offset>                   Konsolenausgabe ab Offset + Status
       op=console_clear
       op=term_open, cols, rows / term_read, since / term_write, data / term_resize, cols, rows / term_close

     @see also ctrl

     TODO
     if [ -d "$HOME/.local/bin" ] ; then
       PATH="$HOME/.local/bin:$PATH"
     fi
    """

    MODULES = ['fabric']

    BASE_PKGS = ['git', 'curl', 'wget']
    NPY_PKGS = ['python3', 'python-is-python3', 'python3-pip', 'npm']

    # Installationsschritte: name -> Titel, Beschreibung, Methode
    OPS = {
        'check': {'title': "System prüfen", 'method': 'check_system',
                  'desc': "Zeigt OS, Kernel, Speicher und installierte Tools an."},
        'ssh_config': {'title': "SSH-Konfiguration", 'method': 'ssh_config',
                       'desc': "~/.ssh anlegen, authorized_keys ergänzen, Git-Hosts (gitlab/github) eintragen."},
        'install_base': {'title': "Basis-Pakete", 'method': 'install_base',
                         'desc': "apt update und Installation von " + ', '.join(BASE_PKGS) + "."},
        'sudo_nopasswd': {'title': "sudo ohne Passwort", 'method': 'sudo_without',
                          'desc': "Legt /etc/sudoers.d/<user> mit NOPASSWD an."},
        'rpi': {'title': "Raspberry Pi", 'method': 'setup_rpi',
                'desc': "i2c-tools installieren und I2C aktivieren (raspi-config)."},
        'setup_python': {'title': "Python", 'method': 'setup_python',
                         'desc': "pip, python-is-python3 und Standard-Module (flask, paho-mqtt, ...)."},
        'pm2': {'title': "PM2", 'method': 'pm2',
                'desc': "npm + pm2 installieren und als systemd-Dienst einrichten."},
        'npy': {'title': "n-py", 'method': 'npy',
                'desc': "nwebclient + nxbot klonen, Update-/Server-Skripte, /etc/nweb.json, Start mit pm2."},
        'startnpy': {'title': "n-py (neu)starten", 'method': 'start_npy',
                     'desc': "Startet bzw. restartet den n-py Server in pm2."},
        'docker': {'title': "Docker", 'method': 'docker',
                   'desc': "Docker per get.docker.com installieren, Benutzer zur Gruppe docker hinzufügen."},
    }

    host = ''
    user = None
    port = 22
    home = None

    # rpi-setup.sh Document
    doc_guid = '7768101250E8D789BDA8B94F860C46DE'

    authorized_keys = [
        'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCybILEUh5HET1vo4gj28jQUDzaxVB4eBOhr0cui3x+MG1FOPb5gwdcfSEP7X5LaAF5ulW5BMFewL7FpdcMvu5U2DNcfnq7Q+/x3rxNBDtWRArXLAjFEEyFIPudxoCFIFpBwMWFuSFdovzhJampH7jET8CZkQ04hfweRSQuAt8Y/V8j1pZWqA/LxzWFGN7t90VcjSzRiSkJrjHRkgzMXWC30xUkhggnOAae6HxnRsypMG/SmnDBBB5GLGiEVsEHCwhQy1rF8TmyaNSnMrKTP35WCNx79RtTweY4eXqo3kT1MbE+OLnBpqlEC6QbwNFeUr5grRCYVfsIz/IJyJeNJO7Z pi@fog.bsnx.net'
    ]

    connection = None

    XTERM_JS = 'https://cdn.jsdelivr.net/npm/@xterm/xterm@5.5.0/lib/xterm.min.js'
    XTERM_CSS = 'https://cdn.jsdelivr.net/npm/@xterm/xterm@5.5.0/css/xterm.min.css'
    XTERM_FIT_JS = 'https://cdn.jsdelivr.net/npm/@xterm/addon-fit@0.10.0/lib/addon-fit.min.js'

    def __init__(self, type='sshbot', args: u.Args = {}):
        super().__init__('sshbot')
        self.args = args
        self.sudo_pw = None
        self.remote_info = {}
        self._console = _TextBuffer()
        self._out = _ConsoleWriter(self.log)
        self.op_state = {name: {'status': 'idle', 'run': 0} for name in self.OPS}
        self.op_logs = {name: _TextBuffer() for name in self.OPS}  # Ausgabe des letzten Laufs je Schritt
        self._run_counter = 0
        self._op_log = None
        self.current_op = None
        self._op_thread = None
        self._op_lock = threading.Lock()
        self._failed_cmds = 0
        # interaktives Terminal
        self.term_buffer = _TextBuffer(max_chars=400000)
        self._term_channel = None
        self._term_thread = None
        self.define_vars('current_op')

    @property
    def ssh_stdout(self):
        """ Kompatibilitaet: gesamte Konsolenausgabe """
        return self._console.text()

    # ------------------------------------------------------------------ Jobs

    @r.op("Konsolenausgabe ab Offset since, dazu Status der Installationsschritte",
          success={'data': str, 'next': int, 'current_op': str, 'ops': dict, 'connected': bool, 'terminal': bool})
    def console(self, since: int = 0):
        text, nxt = self._console.read(since)
        return self.success('console', data=text, next=nxt, current_op=self.current_op, ops=self.op_state,
                            connected=self.is_connected(), terminal=self.is_term_open())

    @r.op("Ausgabe der Installationsschritte, since: {name: [run, offset]} (JSON), "
          "liefert je Schritt mit neuer Ausgabe {data, next, run, reset}",
          success={'outputs': dict, 'ops': dict, 'current_op': str, 'connected': bool})
    def ops_output(self, since: dict = None):
        since = since or {}
        outputs = {}
        for name, buffer in self.op_logs.items():
            run = self.op_state[name].get('run', 0)
            client_run, offset = (list(since.get(name, [])) + [0, 0])[:2]
            reset = int(client_run) != run
            text, nxt = buffer.read(0 if reset else int(offset))
            if text or reset:
                outputs[name] = {'data': text, 'next': nxt, 'run': run, 'reset': reset}
        return self.success('ops_output', outputs=outputs, ops=self.op_state, current_op=self.current_op,
                            connected=self.is_connected())

    @r.op("Leert die Konsolenausgabe", success={}, fail={})
    def console_clear(self):
        self._console.clear()
        return self.success('cleared')

    # ------------------------------------------------------------ Verbindung

    def is_connected(self) -> bool:
        try:
            return self.connection is not None and self.connection.is_connected
        except Exception:
            return False

    @r.op("Verbindet per SSH, danach weiter auf die Seite connection (redirect)",
          success={'uptime': str, 'redirect': str})
    def connect(self, host: str, username: str = 'pi', password: str = '', port: int = 22, key_filename: str = ''):
        """
        :param password: Passwort (auch fuer sudo), leer: SSH-Agent bzw. Schluessel aus ~/.ssh
        :param key_filename: optional, Pfad zu einem privaten Schluessel
        """
        from fabric import Connection
        self.disconnect()
        connect_kwargs = {}
        if password:
            connect_kwargs['password'] = password
        if key_filename:
            connect_kwargs['key_filename'] = key_filename
        self.host = host
        self.user = username or 'root'
        self.port = int(port or 22)
        self.sudo_pw = password or None
        try:
            self.connection = Connection(host, port=self.port, user=self.user, connect_kwargs=connect_kwargs,
                                         connect_timeout=10)
            self.connection.open()
        except Exception as e:
            self.connection = None
            self.log(f'Verbindung zu {username}@{host}:{port} fehlgeschlagen: {e}\n')
            return self.fail(f'Verbindung fehlgeschlagen: {e}')
        self.log(f'### Verbunden mit {self.user}@{self.host}:{self.port}\n')
        self.read_remote_info()
        self.op_state = {name: {'status': 'idle', 'run': 0} for name in self.OPS}
        self.op_logs = {name: _TextBuffer() for name in self.OPS}
        return self.success('connected', uptime=self.remote_info.get('uptime', ''), redirect=self.link('connection'))

    @r.op("Trennt die SSH-Verbindung", success={'redirect': str})
    def disconnect(self):
        self.term_close()
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception:
                pass
            self.log('### Verbindung getrennt\n')
        self.connection = None
        self.remote_info = {}
        return self.success('disconnected', redirect=self.link('index'))

    def read_remote_info(self):
        script = ('echo "$HOME"; whoami; uname -srm; '
                  '(. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME") || echo "?"; '
                  'uptime -p 2>/dev/null || uptime; '
                  'cat /sys/firmware/devicetree/base/model 2>/dev/null | tr -d "\\0"; echo')
        try:
            r = self.connection.run(script, hide=True, warn=True, in_stream=False)
            lines = r.stdout.splitlines() + [''] * 6
            self.home = lines[0].strip() or None
            self.remote_info = {'home': lines[0], 'whoami': lines[1], 'kernel': lines[2], 'os': lines[3],
                                'uptime': lines[4], 'model': lines[5]}
        except Exception as e:
            self.remote_info = {'error': str(e)}

    def _require_connection(self):
        if not self.is_connected():
            raise ConnectionError('Keine SSH-Verbindung')

    # ------------------------------------------------------- Hintergrund-Ops

    def log(self, text: str):
        """ Gesamte Konsole, im Hintergrund-Thread zusaetzlich die Ausgabe des laufenden Schritts """
        self._console.append(text)
        if self._op_log is not None and threading.current_thread() is self._op_thread:
            self._op_log.append(text)

    def is_busy(self) -> bool:
        return self._op_thread is not None and self._op_thread.is_alive()

    @r.op("Startet einen Installationsschritt (siehe OPS) im Hintergrund", name='start', success={'name': str})
    def start_op(self, name: str):
        if name not in self.OPS:
            return self.fail(f'Unbekannter Schritt: {name}')
        method = getattr(self, self.OPS[name]['method'])
        return self._start_background(name, self.OPS[name]['title'], method)

    @r.op("Fuehrt einen Befehl im Hintergrund aus, Ausgabe in der Konsole", name='command', success={'name': str})
    def start_command(self, command: str):
        command = str(command or '').strip()
        if command == '':
            return self.fail('Kein Befehl')
        return self._start_background(None, command, lambda: self.run(command))

    def _start_background(self, name, title, fn):
        if not self.is_connected():
            return self.fail('Keine SSH-Verbindung')
        with self._op_lock:
            if self.is_busy():
                return self.fail(f'Es läuft bereits: {self.current_op}')
            self.current_op = title
            self._op_log = None
            if name is not None:
                self._run_counter += 1
                self._op_log = self.op_logs[name] = _TextBuffer()
                self.op_state[name] = {'status': 'running', 'start': time.time(), 'run': self._run_counter}
            self._op_thread = threading.Thread(target=self._run_background, args=(name, title, fn), daemon=True)
            self._op_thread.start()
        return self.success(f'gestartet: {title}', name=name)

    def _run_background(self, name, title, fn):
        self._failed_cmds = 0
        start = time.time()
        self.log(f'\n### {title}\n')
        status = 'ok'
        try:
            fn()
            if self._failed_cmds > 0:
                status = 'warn'
        except Exception as e:
            status = 'failed'
            self.log(f'!!! Fehler: {e}\n')
            self.error(f'SshBot {title}: {e}')
        duration = round(time.time() - start, 1)
        self.log(f'### {title}: {status} ({duration}s, {self._failed_cmds} fehlgeschlagene Befehle)\n')
        if name is not None:
            self.op_state[name] = {'status': status, 'duration': duration, 'failed': self._failed_cmds,
                                   'run': self.op_state[name].get('run', 0)}
        self.current_op = None

    # --------------------------------------------------------- Ausfuehrung

    def _after(self, result):
        if result is not None and result.exited != 0:
            self._failed_cmds += 1
            self.log(f'[exit {result.exited}]\n')
        return result

    def run(self, command, **kwargs):
        """
        Fuehrt command aus, Ausgabe live in die Konsole. Wirft keine Exception bei Exit-Code != 0.
        https://docs.pyinvoke.org/en/latest/api/runners.html#invoke.runners.Runner.run
        :param kwargs: e.g. env
        :return: invoke Result
        """
        self._require_connection()
        self.log(f'$ {command}\n')
        result = self.connection.run(command, hide=False, warn=True, pty=False, in_stream=False,
                                     out_stream=self._out, err_stream=self._out, **kwargs)
        return self._after(result)

    def run_for_stdout(self, command) -> str:
        self._require_connection()
        r = self.connection.run(command, hide=True, warn=True, in_stream=False)
        return r.stdout

    def sudo(self, command, ignore_error=False, **kwargs):
        if self.user == 'root':
            return self.run(command, **kwargs)
        self._require_connection()
        self.log(f'$ sudo {command}\n')
        try:
            result = self.connection.sudo(command, password=self.sudo_pw, hide=False, warn=True, pty=False,
                                          in_stream=False, out_stream=self._out, err_stream=self._out, **kwargs)
        except Exception as e:
            if ignore_error:
                self.log(f'(ignoriert) {e}\n')
                return None
            raise
        if ignore_error and result.exited != 0:
            self.log(f'[exit {result.exited}, ignoriert]\n')
            return result
        return self._after(result)

    def is_installed(self, package) -> bool:
        return self.run_for_stdout(f'dpkg -s {shlex.quote(package)} >/dev/null 2>&1 && echo yes').strip() == 'yes'

    def apt_install(self, package):
        return self.sudo('DEBIAN_FRONTEND=noninteractive apt-get install -y ' + package)

    def remote_home(self) -> str:
        if not self.home:
            self.home = self.run_for_stdout('echo $HOME').strip()
        return self.home

    # ---------------------------------------------------- Installationsschritte

    def check_system(self):
        for k, v in self.remote_info.items():
            self.log(f'{k}: {v}\n')
        self.run('free -h; df -h / | tail -1')
        for bin_name in ['python', 'pip', 'node', 'npm', 'pm2', 'docker', 'git', 'curl', 'wget']:
            path = self.run_for_stdout(f'command -v {bin_name}').strip()
            self.log(f'{bin_name:8} {path or "-"}\n')
        self.run('ls -1 ~/dev 2>/dev/null || echo "~/dev fehlt"')

    def install_base(self):
        self.sudo('apt-get update')
        self.apt_install(' '.join(self.BASE_PKGS))
        return {'success': True, 'op': 'install_base'}

    def sudo_without(self):
        line = shlex.quote(f'{self.user} ALL=(ALL) NOPASSWD: ALL')
        file = f'/etc/sudoers.d/nxbot-{self.user}'
        self.sudo(f'sh -c "echo {line} > {file} && chmod 440 {file} && visudo -cf {file}"')

    def ssh_config(self):
        self.run('mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys')
        self.ssh_add_host('gitlab_com', 'gitlab.com')
        self.ssh_add_host('github', 'github.com')
        for key in self.authorized_keys:
            q = shlex.quote(key.strip())
            self.run(f'grep -qxF {q} ~/.ssh/authorized_keys || echo {q} >> ~/.ssh/authorized_keys')
        return {'success': True, 'op': 'ssh_config'}

    def ssh_add_host(self, local_key_name, host, user='git'):
        local_key = Path.home() / '.ssh' / local_key_name
        if not local_key.exists():
            self.log(f'Lokaler Schlüssel {local_key} fehlt, {host} übersprungen\n')
            return
        self.run(f'grep -q "^Host {host}$" ~/.ssh/config 2>/dev/null || '
                 f'printf "Host {host}\\n  IdentityFile ~/.ssh/{local_key_name}\\n  User {user}\\n" >> ~/.ssh/config')
        self.log(f'put {local_key} -> ~/.ssh/{local_key_name}\n')
        self.connection.put(str(local_key), f'.ssh/{local_key_name}')
        self.run(f'chmod 600 ~/.ssh/{local_key_name}')
        self.run(f'ssh-keyscan -H {host} >> ~/.ssh/known_hosts')

    def pm2(self):
        self.apt_install('npm')
        self.sudo('npm install pm2 -g')
        self.sudo(f'env PATH=$PATH:/usr/bin:/usr/local/bin pm2 startup systemd -u {self.user} --hp {self.remote_home()}')
        return self.success('pm2')

    def setup_rpi(self):
        self.apt_install('i2c-tools')
        self.sudo('raspi-config nonint do_i2c 0', ignore_error=True)

    def setup_python(self):
        self.apt_install('python3-pip python-is-python3')
        self.sudo('rm -f /usr/lib/python3.*/EXTERNALLY-MANAGED', ignore_error=True)
        self.run('pip install flask paho-mqtt jc beautifulsoup4 pyyaml lxml zeroconf')
        self.run('pip install pyserial')
        return {'success': True, 'op': 'setup_python'}

    def npy(self):
        self.run('mkdir -p ~/dev')
        self.run('cd ~/dev && (test -d nwebclient || git clone git@gitlab.com:bsalgert/nwebclient.git)')
        self.run('cd ~/dev && (test -d nxbot || git clone git@gitlab.com:bsalgert/nxbot.git)')
        # git clone git@gitlab.com:bsalgert/static.git
        self.npy_update()
        # python -m nwebclient.nx serv --rest
        self.write_script('~/npy-server.sh', [
            'git -C ~/dev/nwebclient pull',
            'git -C ~/dev/nxbot pull',
            'python3 -m nwebclient.runner --rest --cfg --executor nwebclient.runner:AutoDispatcher'
        ])
        self.npy_nweb_json()
        self.run('sh ~/npy-update.sh')
        self.start_npy()
        return {'success': True, 'op': 'npy'}

    def start_npy(self):
        self.run('pm2 restart npy || pm2 start ~/npy-server.sh --name npy')
        self.run('pm2 save')

    def write_script(self, path, lines):
        content = '\n'.join(['#!/bin/sh', *lines]) + '\n'
        self.run(f'printf %s {shlex.quote(content)} > {path} && chmod +x {path}')

    def npy_nweb_json(self):
        self.sudo(f'sh -c "test -f /etc/nweb.json || echo \'{{\\"runners\\": {{}}}}\' > /etc/nweb.json"')
        self.sudo(f'chown {self.user}:{self.user} /etc/nweb.json')
        self.run('test -e ~/nweb.json || ln -s /etc/nweb.json ~/nweb.json')
        self.sudo('mkdir -p /etc/nx.d')
        self.run('test -e ~/nx.d || ln -s /etc/nx.d ~/nx.d')

    def npy_update(self):
        self.write_script('~/npy-update.sh', [
            'git -C ~/dev/nwebclient pull',
            'pip install ~/dev/nwebclient',
            'git -C ~/dev/nxbot pull',
            'pip install ~/dev/nxbot'
        ])

    def docker(self):
        self.run('curl -fsSL https://get.docker.com -o get-docker.sh')
        self.sudo('sh get-docker.sh')
        self.sudo('groupadd -f docker')
        self.sudo(f'usermod -aG docker {self.user}')
        return {'success': True, 'op': 'docker'}

    def read_local_keys(self):
        # TODO use
        with open(Path.home() / ".ssh" / "authorized_keys") as file:
            for line in file:
                self.authorized_keys.append(line)

    # -------------------------------------------------------------- Terminal

    def is_term_open(self) -> bool:
        ch = self._term_channel
        return ch is not None and not ch.closed

    @r.op("Oeffnet das interaktive Terminal (PTY + Shell)", success={'next': int})
    def term_open(self, cols: int = 80, rows: int = 24):
        if not self.is_connected():
            return self.fail('Keine SSH-Verbindung')
        ch = self._term_channel
        if self.is_term_open():
            ch.resize_pty(width=cols, height=rows)
            return self.success('terminal already open', next=0)
        self.term_buffer = _TextBuffer(max_chars=400000)
        transport = self.connection.client.get_transport()
        ch = transport.open_session()
        ch.get_pty(term='xterm-256color', width=cols, height=rows)
        ch.invoke_shell()
        self._term_channel = ch
        self._term_thread = threading.Thread(target=self._term_reader, args=(ch, self.term_buffer), daemon=True)
        self._term_thread.start()
        return self.success('terminal opened', next=0)

    def _term_reader(self, ch, buffer: _TextBuffer):
        import codecs
        decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')
        try:
            while True:
                data = ch.recv(4096)
                if not data:
                    break
                buffer.append(decoder.decode(data))
        except Exception as e:
            buffer.append(f'\r\n[Terminal-Fehler: {e}]\r\n')
        buffer.append('\r\n[Terminal beendet]\r\n')
        try:
            ch.close()
        except Exception:
            pass

    @r.op("Liest die Terminal-Ausgabe ab Offset since", success={'data': str, 'next': int, 'open': bool})
    def term_read(self, since: int = 0):
        text, nxt = self.term_buffer.read(since)
        return self.success('term', data=text, next=nxt, open=self.is_term_open())

    @r.op("Sendet Eingaben (Tastendruecke) an das Terminal", success={}, fail={})
    def term_write(self, data: str):
        if not self.is_term_open():
            return self.fail('terminal not open')
        self._term_channel.sendall(data.encode('utf-8'))
        return self.success()

    @r.op("Aendert die Groesse des Terminals", success={}, fail={})
    def term_resize(self, cols: int = 80, rows: int = 24):
        if self.is_term_open():
            self._term_channel.resize_pty(width=cols, height=rows)
        return self.success()

    @r.op("Schliesst das Terminal", success={}, fail={})
    def term_close(self):
        ch = self._term_channel
        self._term_channel = None
        if ch is not None:
            try:
                ch.close()
            except Exception:
                pass
        return self.success('terminal closed')

    # ------------------------------------------------------------------- UI

    def nav(self, p: b.Page, active=None):
        """ obere Navigation, active = Titel der aktuellen Seite """
        p.nav(("Index", self.link(self.part_index), "Verbinden und Übersicht"),
              ("Config", self.link(self.part_config), "Hosts aus ~/.ssh/config"),
              ("Verbindung", self.link(self.part_connection), "Installationsschritte und Terminal"),
              ("Output", self.link(self.part_output), "Gesamte Konsolenausgabe"),
              ("Status", self.link(self.part_status), "Installierte Tools auf dem entfernten System"),
              ("Pakete", self.link(self.part_sysinfo), "Installierte Pakete auf dem entfernten System"),
              active=active)

    @staticmethod
    def parse_ssh_config(text: str) -> list:
        """ Bloecke der SSH-Konfiguration [(patterns, {option: wert})], Optionen klein, erster Wert gilt """
        blocks = []
        current = None
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, _, value = line.replace('=', ' ', 1).partition(' ')
            key, value = key.lower(), value.strip().strip('"')
            if key in ('host', 'match'):
                current = (value.split() if key == 'host' else [], {})
                blocks.append(current)
            elif current is not None:
                current[1].setdefault(key, value)
        return blocks

    @classmethod
    def ssh_config_hosts(cls, path='~/.ssh/config') -> list:
        """ Hosts (ohne Wildcards) aus der SSH-Konfiguration: [{host, hostname, user, port, identityfile}] """
        file = Path(path).expanduser()
        if not file.is_file():
            return []
        blocks = cls.parse_ssh_config(file.read_text(encoding='utf-8', errors='replace'))
        defaults = {}
        for patterns, opts in blocks:
            if '*' in patterns:
                for k, v in opts.items():
                    defaults.setdefault(k, v)
        res = []
        for patterns, opts in blocks:
            for name in patterns:
                if any(c in name for c in '*?!'):
                    continue
                o = {**defaults, **opts}
                res.append({'host': name, 'hostname': o.get('hostname', name), 'user': o.get('user', ''),
                            'port': o.get('port', '22'), 'identityfile': o.get('identityfile', '')})
        return res

    def config_connect_btn(self, e):
        params = {'host': e['host'], 'port': e['port'], 'username': e['user'] or 'pi'}
        hint = f"Verbindet mit {params['username']}@{e['hostname']}:{e['port']} (Host {e['host']})"
        return w.button_js("Connect", f'sshbot_connect_host({json.dumps(params)}, this);', hint=hint)

    def part_config(self, p: b.Page, params={}):
        self.nav(p, "Config")
        with p.section(h="Hosts aus ~/.ssh/config"):
            try:
                hosts = self.ssh_config_hosts()
            except Exception as e:
                p(w.alert_info(f"~/.ssh/config nicht lesbar: {html.escape(str(e))}"))
                return
            col = w.Grid.col
            p.grid(hosts, [col('host', "Host"), col('hostname', "HostName"), col('user', "User"),
                           col('port', "Port"), col('identityfile', "IdentityFile"),
                           col('actions', '', html=True, sortable=False, value=self.config_connect_btn)],
                   params, search=True, empty="Keine Hosts in ~/.ssh/config")
            p('<div id="sshbot_connect_msg" style="color:#c0392b; white-space:pre-wrap"></div>')
            p.script(self.JS_CONNECT.replace('__TYPE__', self.type))

    def part_index(self, p: b.Page, params={}):
        self.nav(p, "Index")
        with p.section(h="SSH Bot"):
            p.p("Mit dem SshBot kann man ein Linux System per SSH einrichten.")
            if self.is_connected():
                p.div(f"Verbunden mit <b>{html.escape(f'{self.user}@{self.host}:{self.port}')}</b> &nbsp; "
                      + w.a("Zur Verbindung &raquo;", self.link('connection')))
                p.hr()
            p(f"""
            <form id="sshbot_connect" class="sshbot_connect" onsubmit="return sshbot_connect(event);">
              <label>Host <input name="host" required placeholder="host oder IP" value="{html.escape(self.host or '')}"></label>
              <label>Port <input name="port" type="number" value="{int(self.port or 22)}"></label>
              <label>Benutzer <input name="username" value="{html.escape(self.user or 'pi')}"></label>
              <label>Passwort <input name="password" type="password" placeholder="leer: SSH-Schlüssel/Agent" autocomplete="off"></label>
              <button type="submit">Verbinden</button>
              <span id="sshbot_connect_msg"></span>
            </form>
            """)
            p.style("""
              .sshbot_connect { display: grid; grid-template-columns: max-content 1fr; gap: 6px 12px; max-width: 520px; }
              .sshbot_connect label { display: contents; }
              .sshbot_connect button { grid-column: 2; justify-self: start; }
              #sshbot_connect_msg { grid-column: 1 / 3; color: #c0392b; white-space: pre-wrap; }
            """)
            p.script(self.JS_CONNECT.replace('__TYPE__', self.type))
        with p.section(h="About"):
            p.p("Voraussetzungen: Anmeldung als root oder sudo (Passwort wird für sudo verwendet).")
            p.p("See also: network, docker")
            p.pre('docker run --rm -it -p 2222:22 -p 27070:7070 -p 28080:8080 -p 20080:80 nxware/ssh')
            p.prop("Base Packages", " ".join(self.BASE_PKGS))
            p.prop("NPy Packages", " ".join(self.NPY_PKGS))
        with p.section(h="Links"):
            p.ul([
                w.a("Verbindung", self.link('connection')),
                w.a("Output", self.link('output')),
                w.a("Remote System Status", self.link('status')),
                w.a("Pakete", self.link('sysinfo')),
                w.a("Debug", self.link('debug')),
            ])

    def part_connection(self, p: b.Page, params={}):
        self.nav(p, "Verbindung")
        if not self.is_connected():
            p.h1("SSH Bot")
            p.div("Keine Verbindung. " + w.a("Verbinden &raquo;", self.link('index')))
            return
        target = html.escape(f'{self.user}@{self.host}:{self.port}')
        info = self.remote_info
        with p.section(h=target):
            p(f"""<div class="sshbot_meta">{html.escape(info.get('os', ''))} &middot; {html.escape(info.get('kernel', ''))}
                    {('&middot; ' + html.escape(info['model'])) if info.get('model') else ''}
                    &middot; {html.escape(info.get('uptime', ''))}</div>
                  <div class="sshbot_actions">
                    <a href="{self.link('index')}">&laquo; SSH Bot</a>
                    <a href="{self.link('status')}">Status</a>
                    <button onclick="sshbot.disconnect()">Trennen</button>
                  </div>""")
        with p.section(h='Installation <span id="sshbot_current" class="sshbot_current"></span>'):
            boxes = ''
            for name, op in self.OPS.items():
                boxes += (f'<div class="sshbot_op" data-op="{name}">'
                          f'<div class="sshbot_op_head"><div class="sshbot_op_info">'
                          f'<div class="sshbot_op_title">{html.escape(op["title"])} <span class="sshbot_badge"></span></div>'
                          f'<div class="sshbot_op_desc">{html.escape(op["desc"])}</div></div>'
                          f'<button onclick="sshbot.start(&quot;{name}&quot;)">Starten</button></div>'
                          f'<pre class="sshbot_op_out"></pre></div>')
            p(f'<div class="sshbot_ops">{boxes}</div>')
        with p.section(h="Terminal"):
            p("""<div class="sshbot_term_bar">
                   <button id="sshbot_term_btn" onclick="sshbot.term.toggle()">Terminal öffnen</button>
                   <span id="sshbot_term_state"></span>
                 </div>
                 <div id="sshbot_term" class="sshbot_term"></div>""")
        p.style(self.CSS_CONNECTION)
        cfg = {'type': self.type, 'xterm_js': self.XTERM_JS, 'xterm_css': self.XTERM_CSS,
               'xterm_fit_js': self.XTERM_FIT_JS, 'index': self.link('index')}
        p.script('window.SSHBOT_CFG = ' + json.dumps(cfg) + ';' + self.JS_CONNECTION)

    def part_status(self, p: b.Page, params={}):
        self.nav(p, "Status")
        p.p("Gibt an welches Tool auf dem entfernten System installiert ist")
        if not self.is_connected():
            p.div("Keine Verbindung.")
            return
        for bin_name in ['python', 'pip', 'pm2', 'curl', 'wget', 'git', 'docker']:
            p.prop(bin_name, html.escape(self.run_for_stdout('command -v ' + bin_name)))
        p.pre(html.escape(self.run_for_stdout('ls -1 ~/dev')))

    def part_sysinfo(self, p: b.Page, params={}):
        self.nav(p, "Pakete")
        if not self.is_connected():
            p.div("Keine Verbindung.")
            return
        for pkg in [*self.BASE_PKGS, *self.NPY_PKGS]:
            p.prop(pkg, self.is_installed(pkg))

    def part_debug(self, p: b.Page, params={}):
        self.nav(p)
        p.h2("Docker")
        p("TODO start container")

    def part_output(self, p: b.Page, params={}):
        self.nav(p, "Output")
        p.h1("SSH Output")
        p.pre(html.escape(self._console.text()))

    JS_CONNECT = """
    function sshbot_do_connect(data, btn, msg) {
      data.set('type', '__TYPE__');
      data.set('op', 'connect');
      btn.disabled = true; msg.style.color = ''; msg.textContent = 'Verbinde ...';
      fetch(location.protocol + '//' + location.host + '/', {method: 'POST', body: data})
        .then(r => r.json()).then(res => {
          btn.disabled = false;
          if (res.success && res.redirect) { location.href = res.redirect; }
          else { msg.style.color = '#c0392b'; msg.textContent = res.message || JSON.stringify(res); }
        }).catch(e => { btn.disabled = false; msg.textContent = String(e); });
    }
    function sshbot_connect(ev) {
      ev.preventDefault();
      const form = ev.target;
      sshbot_do_connect(new URLSearchParams(new FormData(form)), form.querySelector('button'),
                        document.getElementById('sshbot_connect_msg'));
      return false;
    }
    function sshbot_connect_host(params, btn) {
      sshbot_do_connect(new URLSearchParams(params), btn, document.getElementById('sshbot_connect_msg'));
    }
    """

    CSS_CONNECTION = """
      .sshbot_meta { opacity: .7; margin-bottom: 8px; }
      .sshbot_actions { display: flex; gap: 12px; align-items: center; margin-bottom: 12px; }
      .sshbot_ops { display: flex; flex-direction: column; gap: 10px; }
      .sshbot_op { border: 1px solid rgba(128,128,128,.35); border-radius: 6px; padding: 10px; }
      .sshbot_op_head { display: flex; gap: 12px; align-items: center; }
      .sshbot_op_info { flex: 1; }
      .sshbot_op_title { font-weight: bold; display: flex; gap: 8px; align-items: center; }
      .sshbot_op_desc { font-size: .85em; opacity: .8; }
      .sshbot_badge { font-size: .75em; font-weight: normal; padding: 1px 6px; border-radius: 8px; }
      .sshbot_badge.running { background: #2980b9; color: #fff; }
      .sshbot_badge.ok { background: #27ae60; color: #fff; }
      .sshbot_badge.warn { background: #e67e22; color: #fff; }
      .sshbot_badge.failed { background: #c0392b; color: #fff; }
      .sshbot_op.running { border-color: #2980b9; }
      .sshbot_op.failed { border-color: #c0392b; }
      .sshbot_current { font-size: .6em; font-weight: normal; color: #2980b9; }
      .sshbot_op_out { background: #000; color: #ccc; max-height: 320px; overflow: auto; padding: 8px; margin: 8px 0 0;
                       border-radius: 4px; white-space: pre-wrap; word-break: break-all; font-size: 12px;
                       line-height: 1.35; font-family: Menlo, Consolas, "DejaVu Sans Mono", monospace; }
      .sshbot_op_out:empty { display: none; }
      .sshbot_op_out .cmd { color: #5fd7ff; }
      .sshbot_op_out .head { color: #ffd75f; font-weight: bold; }
      .sshbot_op_out .err { color: #ff5f5f; }
      .sshbot_term_bar { display: flex; gap: 10px; align-items: center; margin-bottom: 6px; }
      .sshbot_term { background: #000; border-radius: 4px; }
      .sshbot_term.active { height: 420px; padding: 4px; }
      .sshbot_term_fallback pre { color: #ddd; height: 380px; overflow: auto; margin: 0; padding: 6px;
                                  white-space: pre-wrap; word-break: break-all; font-size: 12px; }
      .sshbot_term_fallback input { width: 100%; font-family: monospace; box-sizing: border-box; }
    """

    JS_CONNECTION = r"""
    (function() {
      const cfg = window.SSHBOT_CFG;
      const url = location.protocol + '//' + location.host + '/';
      const $ = (id) => document.getElementById(id);

      function job(data) {
        const body = new URLSearchParams({type: cfg.type, ...data});
        return fetch(url, {method: 'POST', body: body}).then(r => r.json());
      }

      // ------------------------------------------------- Installationsschritte
      let busy = false, pollTimer = null;
      const since = {};  // name -> [run, offset]
      const LABEL = {running: 'läuft', ok: 'ok', warn: 'Warnungen', failed: 'Fehler', idle: ''};

      function box(name) { return document.querySelector('.sshbot_op[data-op="' + name + '"]'); }

      function appendOutput(name, text) {
        const $out = box(name).querySelector('.sshbot_op_out');
        const atEnd = $out.scrollTop + $out.clientHeight >= $out.scrollHeight - 20;
        const frag = document.createDocumentFragment();
        for (const line of text.split(/(?<=\n)/)) {
          const span = document.createElement('span');
          if (line.startsWith('$ ')) span.className = 'cmd';
          else if (line.startsWith('### ')) span.className = 'head';
          else if (line.startsWith('!!! ') || line.startsWith('[exit')) span.className = 'err';
          span.textContent = line;
          frag.appendChild(span);
        }
        $out.appendChild(frag);
        if (atEnd) $out.scrollTop = $out.scrollHeight;
      }

      function updateOps(res) {
        busy = !!res.current_op;
        $('sshbot_current').textContent = busy ? '▶ ' + res.current_op : '';
        document.querySelectorAll('.sshbot_op').forEach(el => {
          const st = (res.ops[el.dataset.op] || {}).status || 'idle';
          el.className = 'sshbot_op ' + st;
          const badge = el.querySelector('.sshbot_badge');
          badge.className = 'sshbot_badge ' + st;
          badge.textContent = (st in LABEL) ? LABEL[st] : st;
          el.querySelector('button').disabled = busy;
        });
      }

      function poll() {
        clearTimeout(pollTimer);
        job({op: 'ops_output', since: JSON.stringify(since)}).then(res => {
          for (const [name, out] of Object.entries(res.outputs || {})) {
            if (!box(name)) continue;
            if (out.reset) box(name).querySelector('.sshbot_op_out').textContent = '';
            if (out.data) appendOutput(name, out.data);
            since[name] = [out.run, out.next];
          }
          updateOps(res);
          if (!res.connected) {
            $('sshbot_current').textContent = 'Verbindung getrennt';
            return;
          }
          pollTimer = setTimeout(poll, busy ? 400 : 1500);
        }).catch(() => { pollTimer = setTimeout(poll, 3000); });
      }

      // --------------------------------------------------------- Terminal
      const term = {
        xterm: null, fit: null, open: false, since: 0, timer: null,
        queue: '', sending: false, fallback: false,

        toggle() { this.open ? this.close() : this.start(); },

        loadXterm() {
          if (window.Terminal) return Promise.resolve();
          const css = document.createElement('link');
          css.rel = 'stylesheet'; css.href = cfg.xterm_css;
          document.head.appendChild(css);
          const load = (src) => new Promise((ok, fail) => {
            const s = document.createElement('script');
            s.src = src; s.onload = ok; s.onerror = fail;
            document.head.appendChild(s);
          });
          return load(cfg.xterm_js).then(() => load(cfg.xterm_fit_js).catch(() => null));
        },

        setupXterm() {
          const el = $('sshbot_term');
          el.innerHTML = ''; el.className = 'sshbot_term active';
          this.xterm = new Terminal({cursorBlink: true, convertEol: false, fontSize: 13,
                                     fontFamily: 'Menlo, Consolas, "DejaVu Sans Mono", monospace',
                                     scrollback: 5000});
          if (window.FitAddon) { this.fit = new FitAddon.FitAddon(); this.xterm.loadAddon(this.fit); }
          this.xterm.open(el);
          if (this.fit) this.fit.fit();
          this.xterm.onData(d => this.send(d));
          this.xterm.onResize(s => job({op: 'term_resize', cols: s.cols, rows: s.rows}));
          this._onResize = () => { if (this.fit) this.fit.fit(); };
          window.addEventListener('resize', this._onResize);
          this.xterm.focus();
        },

        setupFallback() {
          this.fallback = true;
          const el = $('sshbot_term');
          el.className = 'sshbot_term active sshbot_term_fallback';
          el.innerHTML = '<pre></pre><input placeholder="Eingabe + Enter (xterm.js nicht verfügbar) – Strg+C: ^C eingeben">';
          const input = el.querySelector('input');
          input.addEventListener('keydown', ev => {
            if (ev.key === 'Enter') {
              const v = input.value; input.value = '';
              this.send(v === '^C' ? '\x03' : v + '\r');
            }
          });
          input.focus();
        },

        write(text) {
          if (this.xterm) { this.xterm.write(text); return; }
          const pre = $('sshbot_term').querySelector('pre');
          if (!pre) return;
          pre.textContent += text.replace(/\x1b\[[0-9;?]*[ -\/]*[@-~]|\x1b[()][0-9A-B]|\x1b[=>]|\x07|\r/g, '');
          pre.scrollTop = pre.scrollHeight;
        },

        start() {
          $('sshbot_term_btn').disabled = true;
          $('sshbot_term_state').textContent = 'lade ...';
          this.loadXterm().then(() => this.setupXterm()).catch(() => this.setupFallback()).then(() => {
            const cols = this.xterm ? this.xterm.cols : 100, rows = this.xterm ? this.xterm.rows : 30;
            return job({op: 'term_open', cols: cols, rows: rows});
          }).then(res => {
            $('sshbot_term_btn').disabled = false;
            if (!res.success) { $('sshbot_term_state').textContent = res.message; return; }
            this.open = true; this.since = 0;
            $('sshbot_term_btn').textContent = 'Terminal schließen';
            $('sshbot_term_state').textContent = this.fallback ? 'verbunden (einfacher Modus)' : 'verbunden';
            this.poll();
          });
        },

        send(d) {
          this.queue += d;
          this.flush();
        },

        flush() {
          if (this.sending || !this.queue) return;
          const d = this.queue; this.queue = ''; this.sending = true;
          job({op: 'term_write', data: d}).finally(() => {
            this.sending = false;
            this.flush();
            this.poll(true);
          });
        },

        poll(soon) {
          clearTimeout(this.timer);
          if (!this.open) return;
          if (soon) { this.timer = setTimeout(() => this.poll(), 40); return; }
          job({op: 'term_read', since: this.since}).then(res => {
            if (res.data) this.write(res.data);
            this.since = res.next;
            if (!res.open) { this.ended(); return; }
            this.idle = res.data ? 0 : (this.idle || 0) + 1;
            this.timer = setTimeout(() => this.poll(), this.idle < 10 ? 120 : 600);
          }).catch(() => { this.timer = setTimeout(() => this.poll(), 2000); });
        },

        ended() {
          this.open = false;
          $('sshbot_term_btn').textContent = 'Terminal öffnen';
          $('sshbot_term_state').textContent = 'beendet';
        },

        close() {
          job({op: 'term_close'});
          this.ended();
          clearTimeout(this.timer);
          if (this._onResize) window.removeEventListener('resize', this._onResize);
          if (this.xterm) { this.xterm.dispose(); this.xterm = null; }
          const el = $('sshbot_term'); el.innerHTML = ''; el.className = 'sshbot_term';
          this.fallback = false;
        },
      };

      window.sshbot = {
        term: term,
        start(name) {
          job({op: 'start', name: name}).then(res => {
            if (!res.success) appendOutput(name, '!!! ' + (res.message || JSON.stringify(res)) + '\n');
            poll();
          });
        },
        disconnect() {
          if (!confirm('Verbindung trennen?')) return;
          job({op: 'disconnect'}).then(res => { location.href = res.redirect || cfg.index; });
        },
      };

      document.addEventListener('DOMContentLoaded', poll);
      if (document.readyState !== 'loading') poll();
    })();
    """


class Docker(BaseJobExecutor):
    """

    @see https://docker-py.readthedocs.io/en/stable/containers.html#container-objects
    """

    MODULES = ['docker']

    def __init__(self, type='docker', connection=None, args: u.Args={}):
        super().__init__(type)
        import docker
        if connection is None:
            self.client = docker.from_env()
        else:
            self.client = docker.DockerClient(base_url=connection, use_ssh_client=True)

    def __getattr__(self, name):
        """
        Delegiert unbekannte Attribute an self.client (docker.DockerClient), z.B. containers, images, ping()
        """
        if name == 'client' or name.startswith('__'):
            # verhindert Rekursion, solange self.client noch nicht gesetzt ist
            raise AttributeError(name)
        return getattr(self.client, name)

    def __dir__(self):
        return sorted(set(super().__dir__()) | set(dir(self.client)))

    def __contains__(self, item):
        return self.contains_image(item)

    def contains_image(self, name):
        for img in self.client.images.list():
            if name in str(img.labels):
                return True
        return False

    def contains_container(self, name):
        for container in self.client.containers.list(all=True):
            if container.name == name:
                return True
        return False

    def get_container(self, name):
        for container in self.client.containers.list(all=True):
            if container.name == name:
                return container
        return None

    def contains_running_container(self, name):
        for container in self.client.containers.list():
            if container.name == name:
                return True
        return False

    def is_running(self, name):
        return self.contains_running_container(name)

    def execute(self, data):
        if 'contains_image' in data:
            return {'success': self.contains_image(data['contains_image'])}
        if 'run' in data:
            return self.fail('TODO')
        return super().execute(data)

    def run(self, image='ubuntu', name=None, ports={}, volumes=[], environment={}, remove=False):
        # volumes=['/home/user1/:/mnt/vol2','/var/www:/mnt/vol1']
        # restart_policy (dict): Restart the container when it exits.
        #                 Configured as a dictionary with keys:
        #
        #                 - ``Name`` One of ``on-failure``, or ``always``.
        #                 - ``MaximumRetryCount`` Number of times to restart the
        #                   container on failure.
        #
        #                 For example:
        #                 ``{"Name": "on-failure", "MaximumRetryCount": 5}``
        # ports={'2222/tcp': 3333}
        #                    host
        if isinstance(volumes, dict):
            volumes = list(map(lambda itm: f'{itm[0]}:{itm[1]}', volumes.items()))
        res = self.client.containers.run(image, detach=True, name=name, remove=remove, ports=ports, volumes=volumes, environment=environment)
        return res

    def format_ports(self, ports):
        res = []
        for k in ports.keys():
            try:
                v = ports[k]
                hk = f'<span title="Container">{k}</span>'
                hp = '<span title="Host">'+str(v[0]['HostPort'])+'</span>'
                res.append(hk + ':' + hp)
            except:
                pass
        return ', '.join(res)

    def show_container(self, p: base.Page, container):
        # ports '7070/tcp': [{'HostIp': '0.0.0.0', 'HostPort': '27201'}, {'HostIp': '::', 'HostPort': '27201'}]}
        cname = str(container.name)
        p.div(w.a(cname, self.link(self.part_container, c=cname)) + ' ' + self.format_ports(container.ports) + " Image" + str(container.image.tags))

    def part_containers(self, p: b.Page, params=dict()):
        for container in self.client.containers.list(all=True):
            self.show_container(p, container)

    def part_container(self, p: b.Page, params=dict()):
        cname = params['c']
        p.div(cname)
        c = self.client.containers.get(cname)
        self.show_container(p, c)
        runner = self.get_container_runner(cname)
        if runner is None:
            p.pre(DockerContainer.cmd_from_container(c))
            p.div(w.a("Create DockerContainer runner", self.link(self.part_create_runner, c=cname)))
        else:
            p.div(w.a("Open DockerContainer runner", f'?type={runner.type}'))

    def get_container_runner(self, name) -> "DockerContainer | None":
        d = self.getParentClass(r.LazyDispatcher)
        if d is not None:
            for runner in d.get_runner_by_class(DockerContainer):
                if runner.type == name:
                    return runner
        return None

    def part_create_runner(self, p: b.Page, params=dict()):
        cname = params['c']
        if self.get_container_runner(cname) is not None:
            p.alert_success("Runner already exists.")
            return
        with self.opt_dispatcher() as d:
            d.add_runner(DockerContainer.from_container(self.client.containers.get(cname), docker=self))
            p.alert_success("Added.")
        p.div(w.a("Open", f'?type={cname}'))

    def page_run(self, params={}):
        p = b.Page(owner=self)
        p.h1("docker run")
        # TODO
        p.form_input('name', "Name", id='name')
        return p.nxui()

    def part_npy_containers(self, p: base.Page, params={}):
        d = self.getParentClass(r.LazyDispatcher)
        if d is not None:
            rs = d.get_runner_by_class(DockerContainer)
            for itm in rs:
                p.div(itm.type)


    def part_index(self, p: base.Page, params={}):
        # TODO make sure user is in docker group
        with p.section(h="Containers"):
            for container in self.client.containers.list():
                self.show_container(p, container)

        with p.section(h="Npy Managed Containers"):
            self.part_npy_containers(p, params)

        with p.section(h="Docker Instance"):
            p.ul([
                w.a("All Containers", f'?type={self.type}&{self.type}=containers'),
                w.a("Run", f'?type={self.type}&{self.type}=run')
            ])

        with p.section(h="Useful Container"):
            p.ul([
                'ubuntu',
                'nxware/nxdev'
            ])


class SterlingPdf(BaseJobExecutor):
    """
    docker run -d -p 7002:8080 \
  -v ./trainingData:/usr/share/tessdata \
  -v ./extraConfigs:/configs \
  -v ./logs:/logs \
# Optional customization (not required)
# -v /location/of/customFiles:/customFiles \
  -e DOCKER_ENABLE_SECURITY=false \
  -e INSTALL_BOOK_AND_ADVANCED_HTML_OPS=false \
  -e LANGS=en_GB \
  --name stirling-pdf \
  stirlingtools/stirling-pdf:latest
    """

    NAME = 'stirling-pdf'
    IMAGE = 'stirlingtools/stirling-pdf:latest'

    def __init__(self):
        super().__init__()
        self.type = self.NAME
        self.docker = Docker()
        if not self.docker.is_running(self.NAME):
            if self.docker.contains_container(self.NAME):
                self.docker.get_container(self.NAME).start()
            else:
                self.run()

    def run(self):
        self.docker.run(self.IMAGE, self.NAME, ports={'8080/tcp': r.Ports.STIRLING_PDF}, environment=dict(
            DISABLE_ADDITIONAL_FEATURES='false'
        ))

    def part_index(self, p: base.Page, params={}):
        p.a("Sterlin-PDF", f'http://127.0.0.1:{r.Ports.STIRLING_PDF}')
        p.prop("PORT", r.Ports.STIRLING_PDF)


class Selenium(BaseJobExecutor):
    """
    https://www.screenshotmachine.com/
    """

    MODULES = ['selenium']

    docker: Docker

    def __init__(self):
        super().__init__('selenium')
        self.docker = Docker()

    def run(self, url, data={}):
        # docker run -d -p 4444:4444 -p 7900:7900 selenium/standalone-firefox:latest
        port = 4444
        c = self.docker.run('selenium/standalone-firefox:latest', remove=True, ports={'4444/tcp': port})
        time.sleep(5)
        from selenium import webdriver
        executor_url = f'http://127.0.0.1:{port}/wd/hub'
        driver = webdriver.Remote(command_executor=executor_url, options=webdriver.FirefoxOptions())
        driver.get(url)
        time.sleep(1)

        result = self.screenshot(driver, data)

        c.stop()
        return self.success('ok', **result)

    def screenshot(self, driver, data):
        img = driver.get_screenshot_as_base64()
        if 'filename' in data:
            driver.save_screenshot(data['filename'])
        return {'image': img, 'success': True}

    def execute(self, data):
        if 'url' in data:
            return self.run(data['url'], data)
        return super().execute(data)

    def part_index(self, p: base.Page, params={}):
        p.input('url', id='url')
        p(self.action_btn_parametric("Screenshot", dict(type=self.type, url='#url', op='screenshot'), w.JS.display_image_in_div))
        p.pre('', id='result')


class Samba(BaseJobExecutor):
    """
    Listet die Freigaben aus /etc/samba/smb.conf auf und kann für deren Pfad ein ProjectDir (npy-dir) anlegen.
    """
    cfg = '/etc/samba/smb.conf'

    def __init__(self):
        super().__init__('samba')
        self.config = configparser.ConfigParser()
        self.config.read(self.cfg)

    def part_adddir(self, p: base.Page, params={}):
        with self.opt_dispatcher() as d:
            from nxrunner.projects.dev import ProjectDir
            d.add_runner(ProjectDir(params['path']))
            p.alert_success("Added.")

    def part_index(self, p: base.Page, params={}):
        for s in self.config.sections():
            if s != 'global' and not s.startswith('print'):
                if 'path' in self.config[s]:
                    with p.section():
                        p.div(f'{s}: {self.config[s]["path"]}')
                        p.right(w.a("create npy-dir", self.link(self.part_adddir, path=self.config[s]["path"])))


class DockerContainer(BaseJobExecutor):
    """
    Verwaltet einen Docker-Container anhand seines "docker run"-Kommandos (Starten, Stoppen, Entfernen, Restart-Policy, Logs, Compose-Labels).
    """

    # Optionen ohne Wert, damit z.B. bei "-d nginx" das Image nicht als Wert von -d gelesen wird
    BOOL_OPTIONS = {'d', 'i', 't', 'P', 'detach', 'interactive', 'tty', 'rm', 'init', 'privileged',
                    'read-only', 'publish-all', 'no-healthcheck', 'oom-kill-disable'}

    RESTART_POLICIES = {
        'no': "Do not restart the container automatically (default).",
        'always': "Always restart the container when it stops. If it is stopped manually, "
                  "it is restarted when the Docker daemon restarts (e.g. after a reboot).",
        'unless-stopped': "Like 'always', but a container that was stopped manually "
                          "stays stopped, even after the Docker daemon restarts.",
        'on-failure': "Restart only if the container exits with an error (exit code != 0). "
                      "Use 'Max Retries' to limit the number of restart attempts.",
    }
    RESTART_POLICY_HELP = ("Restart policy: decides whether Docker restarts the container automatically "
                           "when it exits or when the Docker daemon starts. Can be changed while the container "
                           "is running (docker update --restart); the change takes effect immediately.")
    RESTART_RETRIES_HELP = ("Only for 'on-failure': maximum number of restart attempts. "
                            "0 = unlimited. Docker waits longer between attempts each time (100ms, 200ms, 400ms, ...).")
    RESTART_APPLY_HELP = ("Applies the policy to the container via 'docker update'. The container is not restarted. "
                          "The stored 'docker run' command of this runner is updated too.")

    COMPOSE_PROJECT_LABEL = 'com.docker.compose.project'
    COMPOSE_SERVICE_LABEL = 'com.docker.compose.service'
    COMPOSE_WORKING_DIR_LABEL = 'com.docker.compose.project.working_dir'
    COMPOSE_CONFIG_FILES_LABEL = 'com.docker.compose.project.config_files'

    def __init__(self, cmd, docker: Docker = None, container=None):
        """
        :param cmd: "docker run"-Kommando
        :param docker: Docker-Runner, dessen Client verwendet wird
        :param container: optional bestehender Container (docker.models.containers.Container), fuer die Labels
        """
        super().__init__()
        self._client: Docker = docker
        cmd = cmd.replace("\n", ' ')
        data = self.parse_docker_run(cmd)
        self.cmd = cmd
        self.options = data.get('options', {})
        self.type = self.options.get('name', ['container'])[0]
        self.image = data.get('image')
        self.volumes = self.options.get('v', [])
        self.ports = self.options.get('p', [])
        self.labels = {}
        self.compose_project = None
        self.compose_service = None
        self.compose_working_dir = None
        self.compose_config_files = None
        self.init_read_labels(container)
        self.define_vars('image', 'compose_project', 'compose_service')

    def init_read_labels(self, container=None):
        """
        Liest die Labels aus dem "docker run"-Kommando (-l/--label) und, falls vorhanden, aus dem Container.
        Erkennt Container, die von docker compose erstellt wurden.

        :param container: bestehender Container; ohne Container wird er ueber einen bereits gesetzten Docker-Runner gesucht
        """
        labels = {}
        for label in self.options.get('l', []) + self.options.get('label', []):
            key, _, value = label.partition('=')
            labels[key] = value
        if container is None and self._client is not None:
            try:
                container = self._client.get_container(self.type)
            except Exception as e:
                self.error(f"Reading container labels failed: {e}")
        if container is not None:
            labels.update(container.attrs.get('Config', {}).get('Labels') or {})
        self.labels = labels
        self.compose_project = labels.get(self.COMPOSE_PROJECT_LABEL)
        self.compose_service = labels.get(self.COMPOSE_SERVICE_LABEL)
        self.compose_working_dir = labels.get(self.COMPOSE_WORKING_DIR_LABEL)
        self.compose_config_files = labels.get(self.COMPOSE_CONFIG_FILES_LABEL)

    def is_compose(self) -> bool:
        return self.compose_project is not None

    def title(self) -> str:
        if self.is_compose():
            return f"Docker: {self.type} (Compose: {self.compose_project}/{self.compose_service})"
        return "Docker: " + self.type

    def setupPage(self, p: base.Page):
        # Titel fuer die Unterseiten (settings, logs, ...)
        p.h1(html.escape(self.title()))

    def get_docker(self) -> Docker:
        if self._client is not None:
            return self._client
        d = self.getParentClass(r.LazyDispatcher)
        if d is not None:
            rs = d.get_runner_by_class(Docker)
            if len(rs) > 0:
                self._client = rs[0]
                return self._client
        self._client = Docker()
        return self._client

    @classmethod
    def from_container(cls, container, docker: Docker = None) -> "DockerContainer":
        """
        Erzeugt einen Runner aus einem bestehenden Container (docker.models.containers.Container)
        """
        return cls(cls.cmd_from_container(container), docker=docker, container=container)

    @classmethod
    def cmd_from_container(cls, container) -> str:
        """
        Rekonstruiert ein "docker run"-Kommando aus den Attributen eines Containers
        """
        attrs = container.attrs
        config = attrs.get('Config', {})
        host_config = attrs.get('HostConfig', {})
        options = defaultdict(list)
        options['name'].append(container.name)
        for container_port, bindings in (host_config.get('PortBindings') or {}).items():
            for binding in bindings or []:
                host_ip = binding.get('HostIp', '')
                host = binding.get('HostPort', '')
                if host_ip not in ('', '0.0.0.0'):
                    host = f'{host_ip}:{host}'
                options['p'].append(f'{host}:{container_port}')
        for bind in host_config.get('Binds') or []:
            options['v'].append(bind)
        try:
            image_env = set(container.image.attrs.get('Config', {}).get('Env') or [])
        except Exception:
            image_env = set()
        for env in config.get('Env') or []:
            if env not in image_env:
                options['e'].append(env)
        restart = (host_config.get('RestartPolicy') or {}).get('Name', '')
        if restart not in ('', 'no'):
            options['restart'].append(restart)
        return cls.build_docker_run({
            'flags': ['d'],
            'options': options,
            'image': config.get('Image'),
            'command': []
        })

    def is_running(self):
        return self.get_docker().is_running(self.type)

    def execute_run(self, data):
        if not self.is_running():
            r.ProcessExecutor(self.cmd)
            return self.success()
        return self.success('already running')

    def execute_stop(self, data):
        r.ProcessExecutor('docker stop ' + self.type)
        return self.success()

    def execute_start(self, data):
        r.ProcessExecutor('docker start ' + self.type)
        return self.success()

    def execute_rm(self, data):
        r.ProcessExecutor('docker rm ' + self.type)
        return self.success()

    @staticmethod
    def parse_docker_run(cmd):
        """

        {
         'flags': ['i', 't'],
         'options': {
             'name': ['mycontainer'],
             'v': ['/host:/container']
         },
         'image': 'ubuntu:latest',
         'command': ['bash']
        }

        :param cmd:
        :return:
        """
        if isinstance(cmd, str):
            tokens = shlex.split(cmd)
        else:
            tokens = cmd  # falls schon eine Liste übergeben wurde
        if tokens[:2] != ['docker', 'run']:
            raise ValueError("Command must start with 'docker run'")
        args = tokens[2:]  # alles nach 'docker run'
        parsed = {
            'flags': [],
            'options': defaultdict(list),  # mehrfach erlaubte Optionen wie -v oder -e
            'image': None,
            'command': []
        }
        i = 0
        while i < len(args):
            arg = args[i]
            if arg.startswith('--'):
                if '=' in arg:
                    key, value = arg[2:].split('=', 1)
                    parsed['options'][key].append(value)
                else:
                    key = arg[2:]
                    if key not in DockerContainer.BOOL_OPTIONS and i + 1 < len(args) and not args[i + 1].startswith('-'):
                        i += 1
                        parsed['options'][key].append(args[i])
                    else:
                        parsed['flags'].append(key)
            elif arg.startswith('-'):
                key = arg[1:]
                if len(key) > 1:
                    # Mehrere Flags zusammen z. B. `-it` → i, t
                    for flag in key:
                        parsed['flags'].append(flag)
                else:
                    if key not in DockerContainer.BOOL_OPTIONS and i + 1 < len(args) and not args[i + 1].startswith('-'):
                        i += 1
                        parsed['options'][key].append(args[i])
                    else:
                        parsed['flags'].append(key)
            elif parsed['image'] is None:
                parsed['image'] = arg
            else:
                parsed['command'].append(arg)
            i += 1
        return dict(parsed)

    @staticmethod
    def build_docker_run(parsed):
        parts = ['docker', 'run']
        for key, values in parsed.get('options', {}).items():
            for val in values:
                if len(key) == 1:
                    parts.extend([f'-{key}', val])
                else:
                    parts.append(f'--{key}={val}')
        for flag in parsed.get('flags', []):
            if len(flag) == 1:
                parts.append(f'-{flag}')
            else:
                parts.append(f'--{flag}')
        image = parsed.get('image')
        if image:
            parts.append(image)
        command = parsed.get('command', [])
        parts.extend(command)
        return ' '.join(shlex.quote(p) for p in parts)

    def part_index(self, p: base.Page, params={}):
        with p.section(h=html.escape(self.title())):
            p.pre(self.cmd)
            p.prop("Running", self.is_running())
            if self.is_compose():
                p.prop("Compose Project", html.escape(self.compose_project))
                p.prop("Compose Service", html.escape(str(self.compose_service)))
                p.prop("Compose Dir", html.escape(str(self.compose_working_dir)))
                p.prop("Compose Files", html.escape(str(self.compose_config_files)))
            p.prop("Image", self.image)
            p.prop("Port", self.ports)
            p.prop("Volumes", self.volumes)
            p(self.action_btn(dict(title="Run", type=self.type, op='run')))
            p(self.action_btn(dict(title="Start", type=self.type, op='start')))
            p(self.action_btn(dict(title="Stop", type=self.type, op='stop')))
            p(self.action_btn(dict(title="RM", type=self.type, op='rm')))
            p.div(w.a("Logs", self.link(self.part_logs)))
            p.div(w.a("Settings", self.link(self.part_settings)))

    def part_settings(self, p: base.Page, params={}):
        """
        Zeigt die Einstellungen des Containers (docker inspect)
        """
        def prop(title, value):
            p.prop(title, html.escape(str(value)))

        def items(values):
            p.ul(map(lambda x: html.escape(str(x)), values or []))

        container = self.get_docker().get_container(self.type)
        if container is None:
            p.alert_info(f"Container {self.type} does not exist.")
            p.pre(self.cmd)
            return
        attrs = container.attrs
        config = attrs.get('Config', {})
        host_config = attrs.get('HostConfig', {})
        state = attrs.get('State', {})
        with p.section(h="Container"):
            prop("Name", container.name)
            prop("Id", container.short_id)
            prop("Image", config.get('Image'))
            prop("Created", attrs.get('Created'))
            prop("Status", state.get('Status'))
            prop("Started", state.get('StartedAt'))
            prop("Exit Code", state.get('ExitCode'))
            prop("Restart Count", attrs.get('RestartCount', 0))
        with p.section(h="Restart Policy"):
            self.part_restart_policy(p, host_config)
        with p.section(h="Command"):
            prop("Entrypoint", config.get('Entrypoint'))
            prop("Cmd", config.get('Cmd'))
            prop("Working Dir", config.get('WorkingDir'))
            prop("User", config.get('User'))
        with p.section(h="Ports"):
            ports = []
            for container_port, bindings in (host_config.get('PortBindings') or {}).items():
                for binding in bindings or []:
                    ports.append(f"{binding.get('HostIp') or '0.0.0.0'}:{binding.get('HostPort')} -> {container_port}")
            items(ports)
        with p.section(h="Mounts"):
            items(map(lambda m: f"{m.get('Source')} -> {m.get('Destination')} ({m.get('Type')}, {m.get('Mode') or 'rw'})", attrs.get('Mounts') or []))
        with p.section(h="Environment"):
            items(config.get('Env'))
        with p.section(h="Network"):
            prop("Network Mode", host_config.get('NetworkMode'))
            for net_name, net in ((attrs.get('NetworkSettings') or {}).get('Networks') or {}).items():
                prop(net_name, net.get('IPAddress'))
        with p.section(h="Labels"):
            items(map(lambda kv: f'{kv[0]}={kv[1]}', (config.get('Labels') or {}).items()))
        with p.section(h="docker run"):
            p.pre(self.cmd)

    def part_restart_policy(self, p: base.Page, host_config: dict):
        """
        Formular zum Aendern der Restart Policy (docker update --restart)
        """
        def attr(value):
            return html.escape(str(value), quote=True)

        policy = host_config.get('RestartPolicy') or {}
        current = policy.get('Name') or 'no'
        retries = policy.get('MaximumRetryCount') or 0
        if host_config.get('AutoRemove'):
            p.alert_info("This container was started with --rm (AutoRemove). "
                         "Docker does not allow a restart policy for such containers.")
            p.prop("Policy", attr(current))
            return
        sel_id = f'{self.type}_restart_policy'
        retries_id = f'{self.type}_restart_retries'
        options = ''.join(
            f'<option value="{attr(name)}" title="{attr(desc)}"{" selected" if name == current else ""}>{attr(name)}</option>'
            for name, desc in self.RESTART_POLICIES.items())
        p.div(f'<span title="{attr(self.RESTART_POLICY_HELP)}" style="width: 150px; display: inline-block;">Policy &#9432;</span>'
              f'<select id="{sel_id}" title="{attr(self.RESTART_POLICIES.get(current, ""))}" '
              f'onchange="this.title = this.options[this.selectedIndex].title; '
              f'document.getElementById(\'{retries_id}\').disabled = this.value !== \'on-failure\';">{options}</select>')
        p.div(f'<span title="{attr(self.RESTART_RETRIES_HELP)}" style="width: 150px; display: inline-block;">Max Retries &#9432;</span>'
              f'<input type="number" min="0" id="{retries_id}" value="{int(retries)}" title="{attr(self.RESTART_RETRIES_HELP)}"'
              f'{"" if current == "on-failure" else " disabled"} />')
        p.div(f'<span title="{attr(self.RESTART_APPLY_HELP)}">' +
              self.action_btn_parametric("Apply", dict(type=self.type, op='restart_policy', policy='#' + sel_id, retries='#' + retries_id)) +
              '</span>')

    def execute_restart_policy(self, data):
        """
        Aendert die Restart Policy des laufenden Containers, entspricht "docker update --restart=..."

        :param data: policy (no|always|unless-stopped|on-failure), retries (nur bei on-failure)
        """
        policy = data.get('policy', 'no')
        if policy not in self.RESTART_POLICIES:
            return self.fail(f"Unknown restart policy: {policy}")
        try:
            retries = int(data.get('retries') or 0) if policy == 'on-failure' else 0
        except ValueError:
            return self.fail("Max Retries must be a number")
        if retries < 0:
            return self.fail("Max Retries must be >= 0")
        container = self.get_docker().get_container(self.type)
        if container is None:
            return self.fail(f"Container {self.type} does not exist")
        restart_policy = {'Name': policy}
        if policy == 'on-failure':
            restart_policy['MaximumRetryCount'] = retries
        try:
            container.update(restart_policy=restart_policy)
        except Exception as e:
            self.error(f"Restart policy update failed: {e}")
            return self.fail(str(e))
        # cmd anpassen, damit ein erneutes "Run" die neue Policy verwendet
        parsed = self.parse_docker_run(self.cmd)
        parsed['options'].pop('restart', None)
        if policy != 'no':
            parsed['options']['restart'].append(f'{policy}:{retries}' if policy == 'on-failure' and retries > 0 else policy)
        self.cmd = self.build_docker_run(parsed)
        self.options = parsed['options']
        return self.success('Restart policy updated', policy=policy, retries=retries, ui_reload=True)

    def part_logs(self, p: base.Page, params={}):
        cmd = f"docker logs {self.type}"
        p.pre(r.ProcessExecutor(cmd).waitForEnd().text())


class Webcams(BaseJobExecutor):
    """
    https://tiagocoutinho.github.io/linuxpy/api/video/
    """
    def __init__(self, type='wcams'):
        super().__init__(type)

    def part_index(self, p: base.Page, params={}):
        p.pre(r.ProcessExecutor('v4l2-ctl --list-devices').waitForEnd().text())
        from linuxpy.video import device as d
        devices = list(d.iter_video_capture_devices())
        p.ul(map(lambda x: str(x.filename) + w.a("Create", self.link(self.part_create, dev=str(x.filename))), devices))
        p.hr()
        p(w.a("About", 'https://tiagocoutinho.github.io/linuxpy/user_guide/video/#information'))

    def part_create(self, p: base.Page, params={}):
        dev = params['dev']
        self.getRoot().jobexecutor.add_runner(Webcam('wcam', dev))
        p(w.a("Open", '?type=wcam'))


class Webcam(BaseJobExecutor):
    """
    Steuert eine V4L2-Webcam (z.B. /dev/video0) über linuxpy, nimmt Fotos auf und zeigt die Kamera-Controls an.
    """
    MODULES = ['linuxpy']

    def __init__(self, type='wcam', dev='/dev/video0'):
        super().__init__(type)
        from linuxpy.video.device import Device
        self.device = Device(dev)

    def execute_open(self, data):
        self.device.open()
        return self.success()

    def execute_photo(self, data):
        self.photo()
        return self.success()

    def photo(self, filename='frame.jpg'):
        from linuxpy.video import device as d
        capture = d.VideoCapture(self.device)
        capture.set_format(640, 480, "MJPG")
        frame = next(iter(self.device))
        with open(filename, "wb") as f:
            f.write(bytes(frame))

    def page_photo(self, params={}):
        return 'TODO'

    def part_index(self, p: base.Page, params={}):
        p(self.action_btn(dict(title="Open", type=self.type, op='open')))
        p(self.action_btn(dict(title="Photo", type=self.type, op='photo')))
        p.pre(str(self.device.info))
        # cam.info.formats
        #>> > cam.info.formats
        #[ImageFormat(type= < BufferType.VIDEO_CAPTURE: 1 >, description = b'Motion-JPEG', flags = < ImageFormatFlag.COMPRESSED: 1 >, pixelformat = < PixelFormat.MJPEG: 1196444237 >),
        #ImageFormat(type= < BufferType.VIDEO_CAPTURE: 1 >, description = b'YUYV 4:2:2', flags = < ImageFormatFlag.0: 0 >, pixelformat = < PixelFormat.YUYV: 1448695129 >)]
        #>> > cam.get_format(BufferType.VIDEO_CAPTURE)
        #Format(width=640, height=480, pixelformat= < PixelFormat.MJPEG: 1196444237 >}
        with p.section(h="Controls"):
            from linuxpy.video import device as d
            for ctrl in self.device.controls.values():
                with p.dv():
                    p.span(ctrl.name)
                    if isinstance(ctrl, d.IntegerControl):
                        # decrease, 'default', 'increase', 'is_flagged_disabled', 'is_flagged_dynamic_array',
                        # is_flagged_execute_on_write', 'is_flagged_grabbed', 'is_flagged_has_payload',
                        # 'is_flagged_inactive', 'is_flagged_modify_layout', 'is_flagged_read_only', 'is_flagged_slider',
                        # 'is_flagged_update', 'is_flagged_volatile', 'is_flagged_write_only', 'is_writeable',
                        # lower_bound, 'set_to_default', 'set_to_maximum', 'set_to_minimum', 'upper_bound', 'value']
                        p.input(ctrl.name, value=str(ctrl.value), type='slider')
                    elif isinstance(ctrl, d.BooleanControl):
                        p(w.checkbox(ctrl.name, "", ctrl.value))
                    p("Default:" + str(w.default))
                p.pre(w.htmlentities(str(ctrl)))
                    # IntegerControl BooleanControl
                    # <BooleanControl white_balance_automatic default=True value=True>
                    # <IntegerControl exposure_time_absolute min=50 max=10000 step=1 default=166 value=166 flags=inactive>
                    #100 microseconds                                   10000 = 1 second

