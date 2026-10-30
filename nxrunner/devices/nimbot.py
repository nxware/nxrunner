"""
Etikettendruck auf einem Niimbot/Nimbot B1 per Bluetooth (RFCOMM oder BLE).

Protokoll analog zu https://github.com/AndBondStyle/niimprint und
https://github.com/MultiMote/niimbluelib (B1PrintTask)

    from nxrunner.devices.nimbot import NimbotB1
    p = NimbotB1(address='AA:BB:CC:DD:EE:FF')
    p.print_text("Hallo Welt")
"""
import base64
import io
import struct
import time

from nwebclient import base
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d

from nxrunner.devices.bluetooth import BluetoothDevice


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


class NimbotB1(BluetoothDevice):
    """
    Runner fuer den Etikettendrucker Niimbot B1.

    Der Druckkopf ist 384px (48mm @ 203dpi) breit, Bilder werden ggf. skaliert.
    Ein 50x30mm Etikett entspricht ca. 384x240px.

    transport: 'auto' (RFCOMM, bei Fehler BLE), 'rfcomm' oder 'ble'
    """
    MODULES = ['pillow', 'bleak']

    DEVICE_NAME = 'Nimbot B1'
    BLE_CHARACTERISTIC = 'bef8d6c9-9c21-4c9e-b632-bd58c1009f9f'

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
        super().__init__('nimbot_b1',
                         address=args.get('nimbot_address', address),
                         channel=args.get('nimbot_channel', channel),
                         transport=args.get('nimbot_transport', transport))
        self.width_mm = int(args.get('nimbot_width_mm', width_mm))
        self.height_mm = int(args.get('nimbot_height_mm', height_mm))
        self.density = int(args.get('nimbot_density', density))
        self.buffer = bytearray()
        self.define_sig(d.PStr('op', 'print_text'), d.Param('text', 'str'), d.PInt('font_size', 0),
                        *self.print_params()).set_description(
            "Text als Etikett drucken (font_size 0 = automatisch)").r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'print_image'), d.Param('file', 'str'), *self.print_params()).set_description(
            "Bilddatei auf dem Server drucken").r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'print_image'), d.Param('image', 'str'), *self.print_params()).set_description(
            "Bild drucken: URL, Base64 oder data:-URL").r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'info')).set_description(
            "Akku, Seriennummer, Software- und Hardware-Version").r(
            success='bool', message='str', battery='int', serial='str', software_version='float',
            hardware_version='float')

    @staticmethod
    def print_params():
        """ optionale Parameter der Druck-Ops, siehe print_opts """
        return (d.PStr('label', ''), d.PInt('quantity', 1), d.PInt('density', 0), d.PInt('rotate', 0))

    def is_target_device(self, name):
        return name.upper().startswith('B1')

    # ------------------------------------------------------------------
    # Pakete
    # ------------------------------------------------------------------

    def connect(self):
        if self.conn is None:
            self.buffer.clear()
        super().connect()

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
                self.last_error = None
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}"
                raise
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

    def part_index(self, p: base.Page, params={}):
        with p.section(h="Nimbot B1"):
            p.div(f"Adresse: {self.address} ({self.transport}), Etikett: {self.width_mm}x{self.height_mm}mm, Dichte: {self.density}")
            self.part_last_error(p)
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
            p(self.label_table())

    def label_table(self):
        """ Etikettgroessen und passende Bildgroesse in Pixeln (8 px/mm, max. Druckkopfbreite) """
        rows = ''.join(f'<tr><td>{l} mm</td><td>{"x".join(map(str, self.label_size_px(l)))} px</td></tr>'
                       for l in self.LABEL_SIZES)
        return (f'<table style="margin-top: 8px"><tr><th>Etikett</th><th>Bildgroesse</th></tr>{rows}</table>'
                f'<div>{self.DPMM} px/mm, max. Breite {self.PRINTHEAD_PX} px</div>')

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
