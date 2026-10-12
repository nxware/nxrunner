"""
Etikettendruck auf einem Niimbot/Nimbot B1 per Bluetooth (RFCOMM).

Protokoll analog zu https://github.com/AndBondStyle/niimprint und
https://github.com/MultiMote/niimbluelib (B1PrintTask)

    from nxrunner.devices.nimbot import NimbotB1
    p = NimbotB1(address='AA:BB:CC:DD:EE:FF')
    p.print_text("Hallo Welt")
"""
import base64
import io
import queue
import socket
import struct
import threading
import time

from nwebclient import runner as r, base
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d


class NiimbotPacket:

    def __init__(self, type_, data=b''):
        self.type = type_
        self.data = bytes(data)

    def checksum(self):
        c = self.type ^ len(self.data)
        for b in self.data:
            c ^= b
        return c

    def to_bytes(self):
        return bytes([0x55, 0x55, self.type, len(self.data)]) + self.data + bytes([self.checksum(), 0xAA, 0xAA])

    @classmethod
    def parse_buffer(cls, buf: bytearray):
        """ Entnimmt vollstaendige Pakete aus buf, gibt Liste der Pakete zurueck """
        packets = []
        while True:
            start = buf.find(b'\x55\x55')
            if start < 0:
                # ein einzelnes 0x55 am Ende kann der Anfang des naechsten Pakets sein
                del buf[:-1 if buf.endswith(b'\x55') else len(buf)]
                break
            if start > 0:
                del buf[:start]
            if len(buf) < 4:
                break
            size = buf[3]
            total = 4 + size + 3
            if len(buf) < total:
                break
            raw = bytes(buf[:total])
            del buf[:total]
            pkt = cls(raw[2], raw[4:4 + size])
            if raw[-2:] == b'\xAA\xAA' and pkt.checksum() == raw[4 + size]:
                packets.append(pkt)
        return packets

    def __repr__(self):
        return f"<NiimbotPacket type=0x{self.type:02X} data={self.data.hex()}>"


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
            raise ConnectionError("Verbindung zum Drucker getrennt")
        return chunk

    def close(self):
        self.sock.close()


class BleTransport:
    """ Bluetooth Low Energy via bleak, kein Koppeln noetig """

    CHARACTERISTIC = 'bef8d6c9-9c21-4c9e-b632-bd58c1009f9f'

    def __init__(self, address, timeout=15):
        import asyncio
        from bleak import BleakClient
        self.asyncio = asyncio
        self.queue = queue.Queue()
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        self.client = BleakClient(address, timeout=timeout)
        try:
            self.run(self.client.connect(), timeout + 5)
            self.run(self.client.start_notify(self.CHARACTERISTIC, lambda _, data: self.queue.put(bytes(data))))
        except BaseException:
            self.close()
            raise

    def run(self, coro, timeout=30):
        return self.asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def write(self, data):
        size = max(20, (self.client.mtu_size or 23) - 3)
        for i in range(0, len(data), size):
            self.run(self.client.write_gatt_char(self.CHARACTERISTIC, data[i:i + size], response=False))
        # Drucker nicht ueberfluten
        time.sleep(0.01)

    def read(self, timeout):
        try:
            return self.queue.get(timeout=max(0.05, timeout))
        except queue.Empty:
            if not self.client.is_connected:
                raise ConnectionError("Verbindung zum Drucker getrennt")
            return None

    def close(self):
        try:
            if self.client.is_connected:
                self.run(self.client.disconnect())
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)


class NimbotB1(r.BaseJobExecutor):
    """
    Runner fuer den Etikettendrucker Niimbot B1.

    Der Druckkopf ist 384px (48mm @ 203dpi) breit, Bilder werden ggf. skaliert.
    Ein 50x30mm Etikett entspricht ca. 384x240px.

    transport: 'auto' (RFCOMM, bei Fehler BLE), 'rfcomm' oder 'ble'
    """
    MODULES = ['pillow', 'bleak']

    PRINTHEAD_PX = 384

    # gaengige B1-Etiketten (Breite x Hoehe in mm)
    LABEL_SIZES = ['50x30', '50x20', '50x40', '50x50', '50x80', '40x30', '40x20', '30x20', '30x15', '25x15']
    DPMM = 8  # 203 dpi

    CMD_CONNECT = 0xC1
    CMD_GET_INFO = 0x40
    CMD_HEARTBEAT = 0xDC
    CMD_SET_LABEL_TYPE = 0x23
    CMD_SET_LABEL_DENSITY = 0x21
    CMD_START_PRINT = 0x01
    CMD_END_PRINT = 0xF3
    CMD_START_PAGE_PRINT = 0x03
    CMD_END_PAGE_PRINT = 0xE3
    CMD_SET_DIMENSION = 0x13
    CMD_GET_PRINT_STATUS = 0xA3
    CMD_PRINT_BITMAP_ROW = 0x85
    CMD_PRINT_EMPTY_ROW = 0x84

    INFO_BATTERY = 10
    INFO_DEVICE_SERIAL = 11
    INFO_SOFTWARE_VERSION = 9
    INFO_HARDWARE_VERSION = 12

    ERRORS = {
        0x01: 'Deckel offen', 0x02: 'Kein Papier', 0x03: 'Akku schwach', 0x04: 'Akkufehler',
        0x05: 'Abgebrochen', 0x06: 'Datenfehler', 0x07: 'Ueberhitzt', 0x08: 'Papierausgabefehler',
        0x09: 'Drucker beschaeftigt', 0x0A: 'Kein Druckkopf', 0x0B: 'Temperatur zu niedrig',
        0x0C: 'Druckkopf lose', 0x10: 'Falsches Papier', 0x11: 'Papiertyp setzen fehlgeschlagen',
        0x13: 'Dichte setzen fehlgeschlagen', 0x16: 'Kommunikationsfehler',
    }

    def __init__(self, address=None, channel=1, width_mm=50, height_mm=30, density=3, transport='auto',
                 args: u.Args = {}):
        super().__init__('nimbot_b1')
        self.address = args.get('nimbot_address', address)
        self.channel = int(args.get('nimbot_channel', channel))
        self.transport = args.get('nimbot_transport', transport)
        self.width_mm = int(args.get('nimbot_width_mm', width_mm))
        self.height_mm = int(args.get('nimbot_height_mm', height_mm))
        self.density = int(args.get('nimbot_density', density))
        self.conn = None
        self.buffer = bytearray()
        self.lock = threading.Lock()
        self.define_sig(d.PStr('op', 'print_text'), d.Param('text', 'str'))
        self.define_sig(d.PStr('op', 'print_image'), d.Param('file', 'str'))
        self.define_sig(d.PStr('op', 'print_image'), d.Param('image', 'str'))
        self.define_sig(d.PStr('op', 'info'))

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------

    def connect(self):
        if self.conn is not None:
            return
        if not self.address:
            raise Exception("Keine Bluetooth-Adresse fuer Nimbot B1 konfiguriert")
        self.buffer.clear()
        if self.transport in ('auto', 'rfcomm'):
            self.info(f"Verbinde mit {self.address} (RFCOMM {self.channel})")
            try:
                self.conn = RfcommTransport(self.address, self.channel)
                return
            except OSError as e:
                if self.transport == 'rfcomm':
                    raise
                self.info(f"RFCOMM fehlgeschlagen ({e}), versuche BLE")
        self.info(f"Verbinde mit {self.address} (BLE)")
        self.conn = BleTransport(self.address)

    def disconnect(self):
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None

    def send(self, pkt: NiimbotPacket):
        self.conn.write(pkt.to_bytes())

    def receive(self, timeout=10):
        deadline = time.time() + timeout
        while True:
            packets = NiimbotPacket.parse_buffer(self.buffer)
            if packets:
                return packets
            remaining = deadline - time.time()
            if remaining <= 0:
                return []
            chunk = self.conn.read(remaining)
            if chunk is None:
                return []
            self.buffer.extend(chunk)

    def transceive(self, cmd, data=b'', resp_offset=1, timeout=10):
        """ Sendet cmd und wartet auf die Antwort mit Typ cmd+resp_offset """
        self.send(NiimbotPacket(cmd, data))
        resp_type = (cmd + resp_offset) & 0xFF
        deadline = time.time() + timeout
        while time.time() < deadline:
            for pkt in self.receive(deadline - time.time()):
                if pkt.type == 0xDB:
                    self.raise_error(pkt.data[0] if pkt.data else 0)
                if pkt.type == resp_type:
                    return pkt
        raise TimeoutError(f"Keine Antwort auf Kommando 0x{cmd:02X}")

    def raise_error(self, code):
        raise Exception(f"Druckerfehler 0x{code:02X}: {self.ERRORS.get(code, 'unbekannt')}")

    # ------------------------------------------------------------------
    # Kommandos
    # ------------------------------------------------------------------

    def handshake(self):
        try:
            self.transceive(self.CMD_CONNECT, b'\x01', timeout=3)
        except TimeoutError:
            self.info("Keine Antwort auf Connect, fahre fort")

    def get_info(self, key):
        pkt = self.transceive(self.CMD_GET_INFO, bytes([key]), resp_offset=key)
        if key == self.INFO_DEVICE_SERIAL:
            return pkt.data.hex()
        if key in (self.INFO_SOFTWARE_VERSION, self.INFO_HARDWARE_VERSION):
            return int.from_bytes(pkt.data, 'big') / 100
        return int.from_bytes(pkt.data, 'big')

    def set_label_type(self, n=1):
        return bool(self.transceive(self.CMD_SET_LABEL_TYPE, bytes([n]), 16).data[0])

    def set_label_density(self, n):
        return bool(self.transceive(self.CMD_SET_LABEL_DENSITY, bytes([n]), 16).data[0])

    def start_print(self, total_pages=1, page_color=0):
        data = struct.pack('>H', total_pages) + b'\x00\x00\x00\x00' + bytes([page_color])
        return bool(self.transceive(self.CMD_START_PRINT, data).data[0])

    def end_print(self):
        return bool(self.transceive(self.CMD_END_PRINT, b'\x01').data[0])

    def start_page_print(self):
        return bool(self.transceive(self.CMD_START_PAGE_PRINT, b'\x01').data[0])

    def end_page_print(self):
        return bool(self.transceive(self.CMD_END_PAGE_PRINT, b'\x01').data[0])

    def set_dimension(self, rows, cols, quantity=1):
        return bool(self.transceive(self.CMD_SET_DIMENSION, struct.pack('>HHH', rows, cols, quantity)).data[0])

    def get_print_status(self):
        data = self.transceive(self.CMD_GET_PRINT_STATUS, b'\x01', 16).data
        page, progress1, progress2 = struct.unpack('>HBB', data[:4])
        if len(data) == 10 and data[6]:
            self.raise_error(data[6])
        return {'page': page, 'progress1': progress1, 'progress2': progress2}

    # ------------------------------------------------------------------
    # Bild
    # ------------------------------------------------------------------

    def parse_label(self, label=None):
        """ '50x30' -> (50, 30), ohne Angabe die konfigurierte Groesse """
        if not label:
            return self.width_mm, self.height_mm
        width, height = str(label).lower().replace(' ', '').split('x')
        return int(width), int(height)

    def label_size_px(self, label=None):
        width_mm, height_mm = self.parse_label(label)
        return min(width_mm * self.DPMM, self.PRINTHEAD_PX), height_mm * self.DPMM

    def prepare_image(self, img, rotate=0, label=None):
        """ label: Bild unter Beibehaltung des Seitenverhaeltnisses mittig auf das Etikett einpassen """
        from PIL import Image, ImageOps
        if rotate:
            img = img.rotate(-int(rotate), expand=True)
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGBA')
            bg = Image.new('RGBA', img.size, (255, 255, 255, 255))
            img = Image.alpha_composite(bg, img)
        if label:
            size = self.label_size_px(label)
            img = ImageOps.contain(img.convert('L'), size)
            canvas = Image.new('L', size, 255)
            canvas.paste(img, ((size[0] - img.width) // 2, (size[1] - img.height) // 2))
            img = canvas
        if img.width > self.PRINTHEAD_PX:
            img = img.resize((self.PRINTHEAD_PX, int(img.height * self.PRINTHEAD_PX / img.width)))
        # Schwarz = 1
        return ImageOps.invert(img.convert('L')).convert('1')

    def encode_image(self, img):
        empty = bytes((img.width + 7) // 8)
        for y in range(img.height):
            line = img.crop((0, y, img.width, y + 1)).tobytes()
            if line == empty:
                yield NiimbotPacket(self.CMD_PRINT_EMPTY_ROW, struct.pack('>HB', y, 1))
            else:
                yield NiimbotPacket(self.CMD_PRINT_BITMAP_ROW, struct.pack('>H', y) + self.pixel_counts(line) + b'\x01' + line)

    def pixel_counts(self, line):
        """ Anzahl schwarzer Pixel, aufgeteilt auf drei Druckkopf-Segmente (wie niimbluelib) """
        chunk = self.PRINTHEAD_PX // 8 // 3
        if len(line) <= chunk * 3:
            return bytes(min(255, sum(bin(b).count('1') for b in line[i * chunk:(i + 1) * chunk])) for i in range(3))
        total = sum(bin(b).count('1') for b in line)
        return b'\x00' + struct.pack('>H', total)

    def render_text(self, text, font_size=None, font=None, label=None):
        from PIL import Image, ImageDraw, ImageFont
        width, height = self.label_size_px(label)
        lines = str(text).replace('\\n', '\n').split('\n')
        if font_size is None:
            font_size = max(12, min(80, int(height / max(1, len(lines)) * 0.7)))
        try:
            fnt = ImageFont.truetype(font or 'DejaVuSans.ttf', int(font_size))
        except OSError:
            fnt = ImageFont.load_default()
        img = Image.new('L', (width, height), 255)
        draw = ImageDraw.Draw(img)
        line_h = int(font_size * 1.15)
        y = (height - line_h * len(lines)) // 2
        for line in lines:
            tw = draw.textlength(line, font=fnt) if hasattr(draw, 'textlength') else draw.textsize(line, font=fnt)[0]
            draw.text(((width - tw) // 2, y), line, font=fnt, fill=0)
            y += line_h
        return img

    def print_image(self, img, quantity=1, density=None, rotate=0, label=None):
        img = self.prepare_image(img, rotate, label)
        quantity = int(quantity)
        with self.lock:
            try:
                self.connect()
                self.handshake()
                self.set_label_density(int(density or self.density))
                self.set_label_type(1)
                self.start_print(quantity)
                self.start_page_print()
                self.set_dimension(img.height, img.width, quantity)
                for pkt in self.encode_image(img):
                    self.send(pkt)
                self.end_page_print()
                time.sleep(0.3)
                deadline = time.time() + 30 + 10 * quantity
                status = None
                while time.time() < deadline:
                    status = self.get_print_status()
                    if status['page'] >= quantity:
                        break
                    time.sleep(0.3)
                else:
                    raise TimeoutError(f"Druck nicht abgeschlossen, letzter Status: {status}")
                self.end_print()
            finally:
                self.disconnect()

    def print_text(self, text, quantity=1, font_size=None, label=None, **kwargs):
        self.print_image(self.render_text(text, font_size, label=label), quantity, **kwargs)

    @staticmethod
    def load_image(data):
        from PIL import Image
        if 'file' in data:
            return Image.open(data['file'])
        s = data['image']
        if s.startswith('http://') or s.startswith('https://'):
            import requests
            return Image.open(io.BytesIO(requests.get(s).content))
        if ',' in s and s.startswith('data:'):
            s = s.split(',', 1)[1]
        return Image.open(io.BytesIO(base64.b64decode(s)))

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------

    def print_opts(self, data):
        return dict(quantity=max(1, int(data.get('quantity') or 1)),
                    density=data.get('density'),
                    rotate=int(data.get('rotate') or 0))

    def execute_print_text(self, data):
        font_size = data.get('font_size')
        self.print_text(data['text'], font_size=int(font_size) if font_size else None, label=data.get('label'),
                        **self.print_opts(data))
        return self.success()

    def execute_print_image(self, data):
        self.print_image(self.load_image(data), label=data.get('label'), **self.print_opts(data))
        return self.success()

    def execute_info(self, data):
        with self.lock:
            try:
                self.connect()
                return self.success(
                    battery=self.get_info(self.INFO_BATTERY),
                    serial=self.get_info(self.INFO_DEVICE_SERIAL),
                    software_version=self.get_info(self.INFO_SOFTWARE_VERSION),
                    hardware_version=self.get_info(self.INFO_HARDWARE_VERSION))
            finally:
                self.disconnect()

    def execute_set_address(self, data):
        self.address = data['address']
        return self.success(address=self.address, ui_reload=True)

    # ------------------------------------------------------------------
    # Koppeln (bluetoothctl)
    # ------------------------------------------------------------------

    @staticmethod
    def bluetoothctl(*commands, wait=2.0, timeout=30):
        """ Fuehrt Kommandos interaktiv in bluetoothctl aus und liefert die Ausgabe """
        import subprocess
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

    @classmethod
    def bt_devices(cls):
        """ Bekannte Bluetooth-Geraete als Liste von (Adresse, Name) """
        import re
        out = cls.bluetoothctl('devices', wait=1)
        found = {}
        for m in re.finditer(r'Device ((?:[0-9A-F]{2}:){5}[0-9A-F]{2}) ([^\r\n]*)', out):
            found[m.group(1)] = m.group(2).strip()
        return sorted(found.items(), key=lambda e: (not cls.is_niimbot(e[1]), e[1]))

    @staticmethod
    def is_niimbot(name):
        return name.upper().startswith('B1')

    def execute_bt_scan(self, data):
        duration = int(data.get('duration', 10))
        self.bluetoothctl('power on', 'scan on', duration, 'scan off', wait=1, timeout=duration + 15)
        devices = self.bt_devices()
        return self.success(devices=[dict(address=a, name=n) for a, n in devices], ui_reload=True)

    def execute_bt_pair(self, data):
        address = data['address']
        out = self.bluetoothctl('agent NoInputNoOutput', 'default-agent', f'pair {address}', 8,
                                f'trust {address}', wait=1, timeout=40)
        if 'Pairing successful' not in out and 'AlreadyExists' not in out:
            return self.fail('Koppeln fehlgeschlagen', address=address, output=out[-2000:])
        self.address = address
        return self.success(address=address, output=out[-2000:], ui_reload=True)

    def execute_bt_remove(self, data):
        out = self.bluetoothctl(f"remove {data['address']}", wait=1)
        return self.success(output=out[-1000:], ui_reload=True)

    def part_install(self, p: base.Page, params={}):
        with p.section(h="Nimbot B1 koppeln"):
            p.div(f"Aktuelle Adresse: {self.address}")
            p.div("Drucker einschalten und nach Geraeten suchen (dauert ca. 10 s).")
            p(self.action_btn(dict(title="Suchen", type=self.type, op='bt_scan')))
        with p.section(h="Geraete"):
            try:
                devices = self.bt_devices()
            except OSError as e:
                p.div(f"bluetoothctl nicht verfuegbar: {e}")
                devices = []
            for address, name in devices:
                label = f"{name} ({address})" + (" - Nimbot" if self.is_niimbot(name) else "")
                p.div(label
                      + self.action_btn(dict(title="Koppeln", type=self.type, op='bt_pair', address=address))
                      + self.action_btn(dict(title="Verwenden", type=self.type, op='set_address', address=address))
                      + self.action_btn(dict(title="Entfernen", type=self.type, op='bt_remove', address=address)))
        p.ul([w.a("Zurueck", self.link(self.part_index))])

    def part_index(self, p: base.Page, params={}):
        with p.section(h="Nimbot B1"):
            p.div(f"Adresse: {self.address} ({self.transport}), Etikett: {self.width_mm}x{self.height_mm}mm, Dichte: {self.density}")
            p.ul([w.a("Drucker koppeln", self.link(self.part_install))])
            p.form_input("address", "Bluetooth-Adresse", id='address')
            p(self.action_btn_parametric("Adresse setzen", dict(type=self.type, op='set_address', address='#address')))
            p(self.action_btn(dict(title="Info", type=self.type, op='info')))
        with p.section(h="Text drucken"):
            p.form_input("text", "Text", id='text')
            default = f"{self.width_mm}x{self.height_mm}"
            labels = [default] + [l for l in self.LABEL_SIZES if l != default]
            p.form_elem(w.combo('label', {l + ' mm': l for l in labels}, id='label'), "Etikett")
            p.form_input("quantity", "Anzahl", id='quantity', value='1', type='number', min='1')
            p(self.action_btn_parametric("Drucken", dict(type=self.type, op='print_text', text='#text',
                                                         label='#label', quantity='#quantity')))
        with p.section(h="Bild drucken"):
            p.form_input("image_file", "Bild", id='image_file', type='file', accept='image/*')
            p('<div><img id="image_preview" style="max-width: 384px; max-height: 240px; border: 1px solid #ccc; display: none"/></div>')
            p.form_elem(w.combo('image_label', {l + ' mm': l for l in labels}, id='image_label'), "Etikett")
            p.form_elem(w.combo('image_rotate', {'0°': '0', '90°': '90', '180°': '180', '270°': '270'},
                                id='image_rotate'), "Drehen")
            p.form_input("image_quantity", "Anzahl", id='image_quantity', value='1', type='number', min='1')
            p(w.button_js("Drucken", 'nimbot_print_image();'))
            p.script(self.build_image_script())

    def build_image_script(self):
        return f"""
        document.addEventListener("DOMContentLoaded", function() {{
            document.getElementById("image_file").addEventListener("change", function(e) {{
                const file = e.target.files[0];
                const img = document.getElementById("image_preview");
                if (!file) {{ img.style.display = "none"; return; }}
                img.src = URL.createObjectURL(file);
                img.style.display = "block";
            }});
        }});
        function nimbot_print_image() {{
            const file = document.getElementById("image_file").files[0];
            if (!file) {{ alert("Bitte ein Bild auswaehlen"); return; }}
            // im Browser verkleinern, damit der Upload klein bleibt (Etikett max. {self.PRINTHEAD_PX}px)
            const img = new Image();
            img.onload = function() {{
                const max = {self.PRINTHEAD_PX * 2};
                const scale = Math.min(1, max / Math.max(img.width, img.height));
                const canvas = document.createElement("canvas");
                canvas.width = Math.round(img.width * scale);
                canvas.height = Math.round(img.height * scale);
                const ctx = canvas.getContext("2d");
                ctx.fillStyle = "#fff";
                ctx.fillRect(0, 0, canvas.width, canvas.height);
                ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
                let data = canvas.toDataURL("image/png");
                if (data.length > 400000) data = canvas.toDataURL("image/jpeg", 0.9);
                URL.revokeObjectURL(img.src);
                exec_job_p({{type: "{self.type}", op: "print_image", image: data,
                    label: "#image_label", rotate: "#image_rotate", quantity: "#image_quantity"}});
            }};
            img.src = URL.createObjectURL(file);
        }}
        """
