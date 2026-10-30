"""
Allgemeine Bluetooth-Funktionen fuer Geraete-Runner: Verbindung per RFCOMM oder BLE,
Suchen und Koppeln per bluetoothctl inkl. UI (part_install).

    class MyDevice(BluetoothDevice):
        BLE_CHARACTERISTIC = '...'

        def __init__(self, address=None):
            super().__init__('my_device', address)
"""
import html
import queue
import re
import socket
import subprocess
import threading
import time

from nwebclient import runner as r, base
from nwebclient import web as w
from nwebclient import dev as d


class RfcommTransport:
    """ Bluetooth Classic (Serial Port Profile) """

    def __init__(self, address, channel=1, timeout=10):
        self.sock = socket.socket(socket.AF_BLUETOOTH, socket.SOCK_STREAM, socket.BTPROTO_RFCOMM)
        self.sock.settimeout(timeout)
        try:
            self.sock.connect((address, channel))
        except OSError:
            self.sock.close()
            raise

    def write(self, data):
        self.sock.sendall(data)

    def read(self, timeout):
        """ Liefert empfangene Bytes oder None bei Timeout """
        self.sock.settimeout(max(0.05, timeout))
        try:
            chunk = self.sock.recv(1024)
        except socket.timeout:
            return None
        if not chunk:
            raise ConnectionError("Verbindung zum Geraet getrennt")
        return chunk

    def close(self):
        self.sock.close()


class BleTransport:
    """ Bluetooth Low Energy via bleak (write + notify auf einer Characteristic), kein Koppeln noetig """

    def __init__(self, address, characteristic, timeout=15, write_delay=0.01):
        import asyncio
        from bleak import BleakClient
        self.asyncio = asyncio
        self.characteristic = characteristic
        self.write_delay = write_delay
        self.queue = queue.Queue()
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        self.client = BleakClient(address, timeout=timeout)
        try:
            self.run(self.client.connect(), timeout + 5)
            self.run(self.client.start_notify(characteristic, lambda _, data: self.queue.put(bytes(data))))
        except BaseException:
            self.close()
            raise

    def run(self, coro, timeout=30):
        return self.asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def write(self, data):
        size = max(20, (self.client.mtu_size or 23) - 3)
        for i in range(0, len(data), size):
            self.run(self.client.write_gatt_char(self.characteristic, data[i:i + size], response=False))
        # Geraet nicht ueberfluten
        time.sleep(self.write_delay)

    def read(self, timeout):
        try:
            return self.queue.get(timeout=max(0.05, timeout))
        except queue.Empty:
            if not self.client.is_connected:
                raise ConnectionError("Verbindung zum Geraet getrennt")
            return None

    def close(self):
        try:
            if self.client.is_connected:
                self.run(self.client.disconnect())
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)


class BluetoothDevice(r.BaseJobExecutor):
    """
    Basis fuer Runner, die ein Geraet per Bluetooth ansprechen.

    transport: 'auto' (RFCOMM, bei Fehler BLE), 'rfcomm' oder 'ble'
    """
    MODULES = ['bleak']

    DEVICE_NAME = 'Bluetooth-Geraet'
    # Characteristic fuer BLE (write + notify), None = kein BLE
    BLE_CHARACTERISTIC = None

    def __init__(self, type, address=None, channel=1, transport='auto'):
        super().__init__(type)
        self.address = address
        self.channel = int(channel)
        self.transport = transport
        self.conn = None
        self.lock = threading.Lock()
        self.scan_results = {}
        self.last_bt_output = ''
        self.last_error = None
        self.define_sig(d.PStr('op', 'set_address'), d.PStr('address')).set_description(
            "Bluetooth-Adresse des Geraets setzen").r(success='bool', message='str', address='str')
        self.define_sig(d.PStr('op', 'bt_power')).set_description(
            "Bluetooth einschalten (bluetoothctl power on)").r(success='bool', message='str', output='str')
        self.define_sig(d.PStr('op', 'bt_scan'), d.PInt('duration', 10)).set_description(
            "Geraete suchen (duration in s)").r(success='bool', message='str', devices='list')
        self.define_sig(d.PStr('op', 'bt_disconnect'), d.PStr('address')).set_description(
            "Verbindung zum Geraet trennen").r(success='bool', message='str', output='str')
        self.define_sig(d.PStr('op', 'bt_device_info'), d.PStr('address')).set_description(
            "bluetoothctl info zum Geraet").r(success='bool', message='str', output='str')
        f = self.define_sig(d.PStr('op', 'bt_pair'), d.PStr('address')).set_description(
            "Geraet koppeln, vertrauen und als Adresse setzen")
        f.r_success(address='str', output='str')
        f.r_fail(address='str', output='str')
        self.define_sig(d.PStr('op', 'bt_remove'), d.PStr('address')).set_description(
            "Kopplung entfernen").r(success='bool', message='str', output='str')

    def is_target_device(self, name):
        """ Ueberschreiben, um passende Geraete in der Liste hervorzuheben """
        return False

    # ------------------------------------------------------------------
    # Verbindung
    # ------------------------------------------------------------------

    def connect(self):
        if self.conn is not None:
            return
        if not self.address:
            raise Exception(f"Keine Bluetooth-Adresse fuer {self.DEVICE_NAME} konfiguriert")
        use_ble = self.transport == 'ble' or (self.transport == 'auto' and self.BLE_CHARACTERISTIC)
        if self.transport in ('auto', 'rfcomm'):
            self.info(f"Verbinde mit {self.address} (RFCOMM {self.channel})")
            try:
                self.conn = RfcommTransport(self.address, self.channel)
                return
            except OSError as e:
                if not use_ble:
                    raise
                self.info(f"RFCOMM fehlgeschlagen ({e}), versuche BLE")
        self.info(f"Verbinde mit {self.address} (BLE)")
        self.conn = BleTransport(self.address, self.BLE_CHARACTERISTIC)

    def disconnect(self):
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None

    # ------------------------------------------------------------------
    # bluetoothctl
    # ------------------------------------------------------------------

    @staticmethod
    def bluetoothctl(*commands, wait=2.0, timeout=30):
        """ Fuehrt Kommandos interaktiv in bluetoothctl aus (Zahl = Pause in s) und liefert die Ausgabe """
        proc = subprocess.Popen(['bluetoothctl'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, universal_newlines=True)
        try:
            for cmd in commands:
                if isinstance(cmd, (int, float)):
                    time.sleep(cmd)
                else:
                    proc.stdin.write(cmd + '\n')
                    proc.stdin.flush()
                    time.sleep(wait)
            out, _ = proc.communicate('quit\n', timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate()
        return out

    @staticmethod
    def bluetoothctl_cmd(*args, timeout=15):
        """ bluetoothctl im Kommandomodus, z.B. bluetoothctl_cmd('show') """
        try:
            res = subprocess.run(['bluetoothctl', *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                 universal_newlines=True, timeout=timeout)
            return res.stdout
        except subprocess.TimeoutExpired as e:
            return e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or '')

    @staticmethod
    def parse_devices(out):
        """ Liest 'Device <MAC> <Name>' Zeilen (auch [NEW] aus Scans), ignoriert [CHG]/[DEL] """
        out = re.sub(r'\x1b\[[0-9;]*m', '', out)
        found = {}
        for line in out.splitlines():
            if '[CHG]' in line or '[DEL]' in line:
                continue
            m = re.search(r'Device ((?:[0-9A-F]{2}:){5}[0-9A-F]{2})(?: (.*))?$', line.strip())
            if m:
                found[m.group(1)] = (m.group(2) or '').strip() or m.group(1)
        return found

    def bt_devices(self):
        """ Bekannte und zuletzt gefundene Geraete als Liste von (Adresse, Name), passende zuerst """
        found = dict(self.scan_results)
        found.update(self.parse_devices(self.bluetoothctl_cmd('devices')))
        return sorted(found.items(), key=lambda e: (not self.is_target_device(e[1]), e[1] == e[0], e[1]))

    def bt_status(self):
        """ Zustand des Bluetooth-Adapters, z.B. {'Powered': 'yes', 'Discovering': 'no'} """
        out = self.bluetoothctl_cmd('show')
        if 'No default controller' in out or 'Waiting to connect' in out:
            return {'error': out.strip()}
        status = {}
        for line in out.splitlines():
            if ':' in line:
                key, value = line.strip().split(':', 1)
                if key in ('Name', 'Powered', 'Discoverable', 'Pairable', 'Discovering'):
                    status[key] = value.strip()
        return status

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------

    def execute_set_address(self, data):
        self.address = data['address']
        return self.success(address=self.address, ui_reload=True)

    def execute_bt_power(self, data):
        self.last_bt_output = self.bluetoothctl_cmd('power', 'on')
        return self.success(output=self.last_bt_output, ui_reload=True)

    def execute_bt_scan(self, data):
        duration = int(data.get('duration', 10))
        self.bluetoothctl_cmd('power', 'on')
        out = self.bluetoothctl_cmd('--timeout', str(duration), 'scan', 'on', timeout=duration + 10)
        self.last_bt_output = out
        self.scan_results.update(self.parse_devices(out))
        devices = self.bt_devices()
        return self.success(devices=[dict(address=a, name=n) for a, n in devices], ui_reload=True)

    def execute_bt_disconnect(self, data):
        self.last_bt_output = self.bluetoothctl_cmd('disconnect', data['address'])
        return self.success(output=self.last_bt_output, ui_reload=True)

    def execute_bt_device_info(self, data):
        self.last_bt_output = self.bluetoothctl_cmd('info', data['address'])
        return self.success(output=self.last_bt_output, ui_reload=True)

    def execute_bt_pair(self, data):
        address = data['address']
        out = self.bluetoothctl('agent NoInputNoOutput', 'default-agent', f'pair {address}', 8,
                                f'trust {address}', wait=1, timeout=40)
        self.last_bt_output = out
        if 'Pairing successful' not in out and 'AlreadyExists' not in out:
            return self.fail('Koppeln fehlgeschlagen', address=address, output=out[-2000:])
        self.address = address
        return self.success(address=address, output=out[-2000:], ui_reload=True)

    def execute_bt_remove(self, data):
        self.last_bt_output = self.bluetoothctl_cmd('remove', data['address'])
        self.scan_results.pop(data['address'], None)
        return self.success(output=self.last_bt_output, ui_reload=True)

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def part_last_error(self, p: base.Page, params={}):
        if self.last_error:
            p.div(f"Letzter Fehler: {self.last_error}", style='color: red')

    def part_install(self, p: base.Page, params={}):
        with p.section(h=f"{self.DEVICE_NAME} koppeln"):
            p.div(f"Aktuelle Adresse: {self.address}")
            try:
                status = self.bt_status()
            except OSError as e:
                status = {'error': f"bluetoothctl nicht verfuegbar: {e}"}
            if 'error' in status:
                p.div(f"Bluetooth-Adapter: {status['error']}", style='color: red')
            else:
                p.div("Bluetooth-Adapter: " + ', '.join(f"{k}: {v}" for k, v in status.items()))
                if status.get('Powered') != 'yes':
                    p.div("Adapter ist ausgeschaltet", style='color: red')
            p(self.action_btn(dict(title="Bluetooth einschalten", type=self.type, op='bt_power')))
            p.div("Geraet einschalten und nicht anderweitig (z.B. Handy-App) verbunden lassen, dann suchen (ca. 10 s).")
            p(self.action_btn(dict(title="Suchen", type=self.type, op='bt_scan')))
            self.part_last_error(p)
        with p.section(h="Geraete"):
            try:
                devices = self.bt_devices()
            except OSError:
                devices = []
            if not devices:
                p.div("Keine Geraete gefunden")
            for address, name in devices:
                label = f"{name} ({address})" + (f" - {self.DEVICE_NAME}" if self.is_target_device(name) else "")
                p.div(label
                      + self.action_btn(dict(title="Koppeln", type=self.type, op='bt_pair', address=address))
                      + self.action_btn(dict(title="Verwenden", type=self.type, op='set_address', address=address))
                      + self.action_btn(dict(title="Info", type=self.type, op='bt_device_info', address=address))
                      + self.action_btn(dict(title="Trennen", type=self.type, op='bt_disconnect', address=address))
                      + self.action_btn(dict(title="Entfernen", type=self.type, op='bt_remove', address=address)))
        if self.last_bt_output:
            with p.section(h="Letzte bluetoothctl-Ausgabe"):
                p.pre(html.escape(self.last_bt_output[-3000:]))
        p.ul([w.a("Zurueck", self.link(self.part_index))])
