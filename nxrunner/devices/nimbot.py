"""
Etikettendruck auf einem Niimbot/Nimbot B1 per Bluetooth (RFCOMM).

Protokoll analog zu https://github.com/AndBondStyle/niimprint

    from nxrunner.devices.nimbot import NimbotB1
    p = NimbotB1(address='AA:BB:CC:DD:EE:FF')
    p.print_text("Hallo Welt")
"""
import base64
import io
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
                buf.clear()
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


class NimbotB1(r.BaseJobExecutor):
    """
    Runner fuer den Etikettendrucker Niimbot B1.

    Der Druckkopf ist 384px (48mm @ 203dpi) breit, Bilder werden ggf. skaliert.
    Ein 50x30mm Etikett entspricht ca. 384x240px.
    """
    MODULES = ['pillow']

    PRINTHEAD_PX = 384
    DPMM = 8  # 203 dpi

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

    def __init__(self, address=None, channel=1, width_mm=50, height_mm=30, density=3, args: u.Args = {}):
        super().__init__('nimbot_b1')
        self.address = args.get('nimbot_address', address)
        self.channel = int(args.get('nimbot_channel', channel))
        self.width_mm = int(args.get('nimbot_width_mm', width_mm))
        self.height_mm = int(args.get('nimbot_height_mm', height_mm))
        self.density = int(args.get('nimbot_density', density))
        self.sock = None
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
        if self.sock is not None:
            return
        if not self.address:
            raise Exception("Keine Bluetooth-Adresse fuer Nimbot B1 konfiguriert")
        self.info(f"Verbinde mit {self.address} (RFCOMM {self.channel})")
        self.sock = socket.socket(socket.AF_BLUETOOTH, socket.SOCK_STREAM, socket.BTPROTO_RFCOMM)
        self.sock.settimeout(10)
        self.sock.connect((self.address, self.channel))
        self.buffer.clear()

    def disconnect(self):
        if self.sock is not None:
            try:
                self.sock.close()
            finally:
                self.sock = None

    def send(self, pkt: NiimbotPacket):
        self.sock.sendall(pkt.to_bytes())

    def receive(self, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            packets = NiimbotPacket.parse_buffer(self.buffer)
            if packets:
                return packets
            self.sock.settimeout(max(0.1, deadline - time.time()))
            try:
                chunk = self.sock.recv(1024)
            except socket.timeout:
                break
            if not chunk:
                raise ConnectionError("Verbindung zum Drucker getrennt")
            self.buffer.extend(chunk)
        return []

    def transceive(self, cmd, data=b'', resp_offset=1, timeout=10):
        """ Sendet cmd und wartet auf die Antwort mit Typ cmd+resp_offset """
        self.send(NiimbotPacket(cmd, data))
        resp_type = (cmd + resp_offset) & 0xFF
        deadline = time.time() + timeout
        while time.time() < deadline:
            for pkt in self.receive(deadline - time.time()):
                if pkt.type == 0xDB:
                    raise Exception(f"Druckerfehler: {pkt.data.hex()}")
                if pkt.type == resp_type:
                    return pkt
        raise TimeoutError(f"Keine Antwort auf Kommando 0x{cmd:02X}")

    # ------------------------------------------------------------------
    # Kommandos
    # ------------------------------------------------------------------

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

    def start_print(self, quantity=1):
        data = b'\x00' + struct.pack('>H', quantity) + b'\x00\x00\x00\x00'
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
        return {'page': page, 'progress1': progress1, 'progress2': progress2}

    # ------------------------------------------------------------------
    # Bild
    # ------------------------------------------------------------------

    def label_size_px(self):
        return min(self.width_mm * self.DPMM, self.PRINTHEAD_PX), self.height_mm * self.DPMM

    def prepare_image(self, img, rotate=0):
        from PIL import Image, ImageOps
        if rotate:
            img = img.rotate(-int(rotate), expand=True)
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGBA')
            bg = Image.new('RGBA', img.size, (255, 255, 255, 255))
            img = Image.alpha_composite(bg, img)
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
                # Pixelzaehler werden vom B1 nicht geprueft, daher 0
                yield NiimbotPacket(self.CMD_PRINT_BITMAP_ROW, struct.pack('>H3BB', y, 0, 0, 0, 1) + line)

    def render_text(self, text, font_size=None, font=None):
        from PIL import Image, ImageDraw, ImageFont
        width, height = self.label_size_px()
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

    def print_image(self, img, quantity=1, density=None, rotate=0):
        img = self.prepare_image(img, rotate)
        quantity = int(quantity)
        with self.lock:
            try:
                self.connect()
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
                while time.time() < deadline:
                    if self.get_print_status()['page'] >= quantity:
                        break
                    time.sleep(0.1)
                self.end_print()
            finally:
                self.disconnect()

    def print_text(self, text, quantity=1, font_size=None, **kwargs):
        self.print_image(self.render_text(text, font_size), quantity, **kwargs)

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
        return dict(quantity=int(data.get('quantity', 1)),
                    density=data.get('density'),
                    rotate=int(data.get('rotate', 0)))

    def execute_print_text(self, data):
        font_size = data.get('font_size')
        self.print_text(data['text'], font_size=int(font_size) if font_size else None, **self.print_opts(data))
        return self.success()

    def execute_print_image(self, data):
        self.print_image(self.load_image(data), **self.print_opts(data))
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
            p.div(f"Adresse: {self.address}, Etikett: {self.width_mm}x{self.height_mm}mm, Dichte: {self.density}")
            p.ul([w.a("Drucker koppeln", self.link(self.part_install))])
            p.form_input("address", "Bluetooth-Adresse", id='address')
            p(self.action_btn_parametric("Adresse setzen", dict(type=self.type, op='set_address', address='#address')))
            p(self.action_btn(dict(title="Info", type=self.type, op='info')))
        with p.section(h="Text drucken"):
            p.form_input("text", "Text", id='text')
            p.form_input("quantity", "Anzahl", id='quantity')
            p(self.action_btn_parametric("Drucken", dict(type=self.type, op='print_text', text='#text', quantity='#quantity')))
