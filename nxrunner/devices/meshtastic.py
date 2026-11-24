"""
Meshtastic-Knoten (verschoben aus nxbot.network)

    Config (runners):
        mb: nxrunner.devices.meshtastic:MeshtasticBot(port="/dev/ttyACM0")
        mb: nxrunner.devices.meshtastic:MeshtasticBot(host="192.168.178.50")

Jobs ueber das Mesh (Port 256 mit Kennung nJ, siehe docs/Meshtastic.md):
    {type: mb, op: send, dest: '!a1b2c3d4', job: {type: ..., ...}}
Job-JSON wird mit raw deflate komprimiert, ueber den CryptRunner (type crypt) verschluesselt und bei Bedarf
in mehrere Pakete zerlegt. Der Empfaenger fuehrt den Job aus und schickt das Ergebnis verschluesselt als
Job {type: mb, op: d, ref: <id>, r: <ergebnis>} zurueck.
"""
import base64
import hashlib
import json
import secrets
import struct
import threading
import time
import zlib
from collections import OrderedDict, deque
from datetime import datetime

from nwebclient import runner as r
from nwebclient import base
from nwebclient import web as w
from nwebclient import dev

MDNS_SERVICE = '_meshtastic._tcp.local.'
MAX_MESSAGES = 25

# Jobs und Bilder laufen auf PRIVATE_APP (256): eigene Portnummern (z.B. 370) kennt die Android-App nicht, sie
# sendet und liest sie als Port 0. Eine 2-Byte-Kennung am Paketanfang trennt Jobs, Bilder und andere private Apps.
PRIVATE_PORT = 256
TEXT_PORT = 1  # TEXT_MESSAGE_APP
PORT_NAMES = {'TEXT_MESSAGE_APP': TEXT_PORT, 'PRIVATE_APP': PRIVATE_PORT}
MAX_PAYLOAD = 227                   # 233 Byte Data-Protobuf (DATA_PAYLOAD_LEN) abzueglich Portnummer und Laengenfeld

JOB_PORT = PRIVATE_PORT
JOB_MAGIC = b'nJ'
PROTO_VERSION = 1                   # 1 = raw deflate + Fernet (CryptRunner)
FRAGMENT_HEADER = struct.Struct('<BHBB')   # Version, Nachrichten-ID, Index, Anzahl
FRAGMENT_DATA = MAX_PAYLOAD - len(JOB_MAGIC) - FRAGMENT_HEADER.size
FRAGMENT_TIMEOUT = 300              # s, unvollstaendige Nachrichten verwerfen
MAX_JOBS = 25                       # gemerkte gesendete/empfangene Jobs

IMAGE_PORT = PRIVATE_PORT
IMAGE_MAGIC = b'nI'
IMAGE_VERSION = 1                   # 1 = progressive JPEG, unverschluesselt
IMAGE_HEADER = struct.Struct('<BHHH')      # Version, Bild-ID, Index, Anzahl
IMAGE_DATA = MAX_PAYLOAD - len(IMAGE_MAGIC) - IMAGE_HEADER.size
IMAGE_RESEND = 2                    # Typ-Byte einer Nachforderung
RESEND_HEADER = struct.Struct('<BH')       # 2, Bild-ID, danach Bereiche
RESEND_RANGE = struct.Struct('<HH')        # erstes fehlendes Paket, Anzahl
MAX_RESEND_RANGES = (MAX_PAYLOAD - len(IMAGE_MAGIC) - RESEND_HEADER.size) // RESEND_RANGE.size
IMAGE_MAX_EDGE = 600
MAX_IMAGES = 10                     # gemerkte gesendete/empfangene Bilder


def deflate(data: bytes) -> bytes:
    c = zlib.compressobj(9, zlib.DEFLATED, -15)
    return c.compress(data) + c.flush()


def inflate(data: bytes) -> bytes:
    return zlib.decompress(data, -15)


def split_fragments(token: bytes, msg_id: int) -> list:
    """ Nachricht in Pakete mit FRAGMENT_HEADER zerlegen """
    chunks = [token[i:i + FRAGMENT_DATA] for i in range(0, len(token), FRAGMENT_DATA)] or [b'']
    if len(chunks) > 255:
        raise ValueError(f"Nachricht zu gross: {len(token)} Byte")
    return [JOB_MAGIC + FRAGMENT_HEADER.pack(PROTO_VERSION, msg_id, i, len(chunks)) + c
            for i, c in enumerate(chunks)]


def to_ranges(indices) -> list:
    """ sortierte Indizes -> [(start, anzahl), ...], z.B. [2, 3, 4, 9] -> [(2, 3), (9, 1)] """
    ranges = []
    for i in indices:
        if ranges and ranges[-1][0] + ranges[-1][1] == i:
            ranges[-1] = (ranges[-1][0], ranges[-1][1] + 1)
        else:
            ranges.append((i, 1))
    return ranges


def image_fragment(img_id: int, jpeg: bytes, idx: int, count: int) -> bytes:
    return (IMAGE_MAGIC + IMAGE_HEADER.pack(IMAGE_VERSION, img_id, idx, count)
            + jpeg[idx * IMAGE_DATA:(idx + 1) * IMAGE_DATA])


def prepare_image(data: bytes, max_edge=IMAGE_MAX_EDGE, quality=30, max_bytes=None) -> tuple:
    """
    Bild auf max_edge (laengste Kante) verkleinern und als progressive JPEG ohne Metadaten speichern.
    Ist das Ergebnis groesser als max_bytes, wird die Qualitaet in 5er-Schritten bis 10 gesenkt.

    :return: (jpeg_bytes, (breite, hoehe), qualitaet)
    """
    import io
    from PIL import Image, ImageOps
    im = Image.open(io.BytesIO(data))
    im = ImageOps.exif_transpose(im)          # Handyfotos richtig drehen, danach EXIF verwerfen
    if im.mode not in ('RGB', 'L'):
        im = im.convert('RGB')
    im.thumbnail((max_edge, max_edge), Image.LANCZOS)
    q = int(quality)
    while True:
        out = io.BytesIO()
        im.save(out, 'JPEG', quality=q, progressive=True, optimize=True)
        jpeg = out.getvalue()
        if max_bytes is None or len(jpeg) <= max_bytes or q <= 10:
            return jpeg, im.size, q
        q = max(10, q - 5)


def preview_jpeg(data: bytes):
    """
    Bisher empfangenen Anfang einer progressive JPEG als vollstaendige JPEG liefern (unscharfe Vorschau),
    None wenn noch nichts dekodierbar ist.
    """
    import io
    from PIL import Image, ImageFile
    old = ImageFile.LOAD_TRUNCATED_IMAGES
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    try:
        im = Image.open(io.BytesIO(data))
        im.load()
        out = io.BytesIO()
        im.convert('RGB').save(out, 'JPEG', quality=85)
        return out.getvalue()
    except Exception:
        return None
    finally:
        ImageFile.LOAD_TRUNCATED_IMAGES = old


class MeshtasticBot(r.BaseJobExecutor):
    """
    Verbindet sich per Serial, TCP oder BLE mit einem Meshtastic-Knoten, sendet Textnachrichten und meldet
    empfangene Pakete als Event. Fuehrt verschluesselte Jobs anderer MeshtasticBots aus (Port 256).
    """

    MODULES = ['meshtastic']

    gpio = {1: 0x2, 2: 0x4, 3: 0x8, 4: 0x10, 5: 0x20, 6: 0x40, 7: 0x80, 8: 0x100, 9: 0x200, 10: 0x400, 11: 0x800,
            12: 0x1000, 13: 0x2000, 14: 0x4000, 15: 0x8000, 16: 0x10000, 17: 0x20000, 18: 0x40000, 19: 0x80000,
            20: 0x100000, 21: 0x200000, 22: 0x400000, 23: 0x800000, 24: 0x1000000, 25: 0x2000000, 26: 0x4000000,
            27: 0x8000000, 28: 0x10000000, 29: 0x20000000, 30: 0x40000000, 31: 0x80000000, 32: 0x100000000,
            33: 0x200000000, 34: 0x400000000, 35: 0x800000000, 36: 0x1000000000, 37: 0x2000000000, 38: 0x4000000000,
            39: 0x8000000000, 40: 0x10000000000, 41: 0x20000000000, 42: 0x40000000000, 43: 0x80000000000,
            44: 0x100000000000}

    def __init__(self, host=None, port=None, max_fragments=16, max_age=600, fragment_delay=0.5,
                 image_quality=30, max_image_fragments=200, image_resend_timeout=20, image_min_complete=0.9,
                 image_max_rounds=3):
        """
        :param max_fragments: maximale Pakete je Nachricht (Job oder Ergebnis)
        :param max_age: s, aeltere Nachrichten (Fernet-Zeitstempel des Senders) werden verworfen, None = aus
        :param fragment_delay: s Pause zwischen den Paketen einer Nachricht
        :param image_quality: JPEG-Qualitaet fuer Bilder, 1-95
        :param max_image_fragments: maximale Pakete je Bild (je 218 Byte), die Qualitaet wird notfalls gesenkt
        Jobs und Bilder laufen auf Port 256 (PRIVATE_APP), getrennt durch die Kennung nJ bzw. nI.
        :param image_resend_timeout: s ohne neues Paket, nach denen ein unvollstaendiges Bild geprueft wird
        :param image_min_complete: Anteil des lueckenlosen Anfangs (0-1), ab dem nichts nachgefordert wird
        :param image_max_rounds: maximale Nachforderungen je Bild (Empfaenger) bzw. je Bild und Anfrager (Sender)
        """
        super().__init__('mb')
        self.iface = None
        self.target = None
        self.connect_error = None
        self.last = ''
        self.messages = deque(maxlen=MAX_MESSAGES)
        self.receive_count = 0
        self.max_fragments = int(max_fragments)
        self.max_age = max_age
        self.fragment_delay = float(fragment_delay)
        self.sent_jobs = OrderedDict()          # msg_id -> dict(time, dest, job, fragments, replies)
        self.received_jobs = deque(maxlen=MAX_JOBS)
        self._partial = {}                      # (from, msg_id) -> dict(time, count, parts)
        self._seen = {}                         # sha256(token) -> time, gegen Wiederholung
        self.image_quality = int(image_quality)
        self.max_image_fragments = int(max_image_fragments)
        self.image_resend_timeout = float(image_resend_timeout)
        self.image_min_complete = float(image_min_complete)
        self.image_max_rounds = int(image_max_rounds)
        self._image_watch_running = False
        self.sent_images = OrderedDict()        # img_id -> dict(time, dest, jpeg, size, fragments, sent)
        self.received_images = OrderedDict()    # (from, img_id) -> dict(time, src, count, parts, done)
        self._lock = threading.Lock()
        self.define_vars('receive_count')
        self.define_event('on_message')
        self.define_event('on_result')
        self.define_event('on_image')
        self.define_sig(dev.PStr('message', 'Hello Node')).set_description('Sendet eine Textnachricht über Meshtastic').r(success=bool, message=str)
        self.define_sig(dev.PStr('op', 'send'), dev.PStr('dest', '^all'), dev.PStr('job', '{"type": "ping"}')).set_description(
            f'Job verschlüsselt über Port {JOB_PORT} an einen anderen MeshtasticBot senden').r(success=bool, message=str, id=int, fragments=int)
        if port is not None:
            self.connect_serial(port)
        if host is not None:
            self.connect_tcp(host)

    def sendText(self, message):
        if not self.is_connected():
            return self.fail("Nicht verbunden")
        self.iface.sendText(message)
        return self.success()

    @staticmethod
    def portnum(packet):
        """ Portnummer als int, bekannte Ports liefert meshtastic als Namen (z.B. TEXT_MESSAGE_APP) """
        pn = packet.get('decoded', {}).get('portnum')
        if isinstance(pn, int):
            return pn
        if pn in PORT_NAMES:
            return PORT_NAMES[pn]
        try:
            return int(pn)
        except (TypeError, ValueError):
            pass
        raw = packet.get('raw')
        if raw is not None and hasattr(raw, 'decoded'):
            return int(raw.decoded.portnum)
        return pn

    def onReceive(self, packet, interface=None):
        """called when a packet arrives"""
        if interface is not None and self.iface is not None and interface is not self.iface:
            return  # Paket eines anderen Interfaces im selben Prozess
        try:
            self.receive_count += 1
            self.last = str(packet)
            self.info(f"Received: {packet}")
            pn = self.portnum(packet)
            if pn == PRIVATE_PORT:
                payload = packet['decoded'].get('payload', b'')
                if payload.startswith(JOB_MAGIC):
                    self.receive_fragment(packet)
                elif payload.startswith(IMAGE_MAGIC):
                    self.receive_image_fragment(packet)
                # sonst: andere private App auf Port 256
            elif pn == TEXT_PORT:
                message = packet['decoded']['payload'].decode('utf-8')
                from_id = packet.get('fromId')
                self.last = message
                self.messages.appendleft(dict(time=datetime.now(), from_id=from_id, sender=self.node_name(from_id),
                                              text=message, snr=packet.get('rxSnr'), rssi=packet.get('rxRssi')))
                self.emit_event('on_message', message=f'{message}')
            else:
                self.emit_event('on_message', message=f'{packet}')
        except Exception as e:
            self.error(e)

    # --- Jobs ueber das Mesh (Port 256, Kennung nJ) ---------------------------------------------------

    def has_crypt(self) -> bool:
        root = self.getRoot()
        try:
            return 'crypt' in getattr(root, 'jobexecutor', root)
        except TypeError:
            return False

    def crypt(self, op: str, data: bytes) -> bytes:
        """ Bytes ueber den CryptRunner ver-/entschluesseln (op encrypt_file / decrypt_file) """
        root = self.getRoot()
        ex = getattr(root, 'jobexecutor', root)
        res = ex.execute(dict(type='crypt', op=op, data=base64.b64encode(data).decode('ascii')))
        if not isinstance(res, dict) or not res.get('success', False):
            raise RuntimeError(f"crypt {op}: " + str(res.get('message') if isinstance(res, dict) else res))
        return base64.b64decode(res['value'])

    def encode_message(self, job: dict) -> bytes:
        """ Job -> raw deflate -> Fernet, Ergebnis ist der Fernet-Token als Binaerdaten (ohne Base64) """
        token_b64url = self.crypt('encrypt_file', deflate(json.dumps(job, separators=(',', ':'), default=str).encode()))
        return base64.urlsafe_b64decode(token_b64url)

    def decode_message(self, token: bytes) -> dict:
        """ Umkehrung von encode_message, wirft bei falschem Schluessel, zu alter Nachricht oder ungueltigem JSON """
        plain = self.crypt('decrypt_file', base64.urlsafe_b64encode(token))
        if self.max_age is not None:
            sent = struct.unpack('>Q', token[1:9])[0]  # Fernet-Zeitstempel, durch das HMAC geschuetzt
            if abs(time.time() - sent) > float(self.max_age):
                raise ValueError(f"Nachricht zu alt ({int(time.time() - sent)} s)")
        job = json.loads(inflate(plain).decode('utf-8'))
        if not isinstance(job, dict):
            raise ValueError("Job ist kein JSON-Objekt")
        return job

    def send_job(self, job: dict, dest='^all', ref=None):
        """
        Job verschluesselt an dest senden, das Senden der Pakete laeuft im Hintergrund.

        :param ref: bei Antworten die ID des beantworteten Jobs (nur zur Anzeige)
        :return: dict mit id und fragments
        """
        if not self.is_connected():
            return self.fail("Nicht verbunden")
        try:
            token = self.encode_message(job)
        except Exception as e:
            return self.fail(str(e))
        msg_id = secrets.randbits(16)
        frags = split_fragments(token, msg_id)
        if len(frags) > self.max_fragments:
            return self.fail(f"Nachricht zu gross: {len(frags)} Pakete, erlaubt {self.max_fragments}",
                             fragments=len(frags))
        if ref is None:
            with self._lock:
                self.sent_jobs[msg_id] = dict(time=datetime.now(), dest=dest, job=job, fragments=len(frags),
                                              size=len(token), replies=[])
                while len(self.sent_jobs) > MAX_JOBS:
                    self.sent_jobs.popitem(last=False)
        threading.Thread(target=self._send_fragments, args=(frags, dest), daemon=True).start()
        return self.success(id=msg_id, fragments=len(frags), size=len(token))

    def _send_fragments(self, frags, dest, port=JOB_PORT, progress=None):
        """ :param progress: dict, dessen Feld sent die Anzahl gesendeter Pakete bekommt """
        for i, frag in enumerate(frags):
            if i > 0 and self.fragment_delay > 0:
                time.sleep(self.fragment_delay)
            try:
                self.iface.sendData(frag, destinationId=dest, portNum=port)
            except Exception as e:
                self.error(f"Senden von Paket {i + 1}/{len(frags)} fehlgeschlagen: {e}")
                if progress is not None:
                    progress['error'] = str(e)
                return
            if progress is not None:
                progress['sent'] = i + 1

    # --- Bilder (Port 256, Kennung nI) ----------------------------------------------------------------

    def send_image(self, data: bytes, dest='^all', quality=None):
        """
        Bild verkleinern (laengste Kante 600 px), als progressive JPEG komprimieren und in Paketen senden.
        Die Pakete gehen in Reihenfolge raus, der Empfaenger zeigt schon nach den ersten eine unscharfe Vorschau.
        """
        if not self.is_connected():
            return self.fail("Nicht verbunden")
        try:
            jpeg, size, q = prepare_image(data, quality=quality or self.image_quality,
                                          max_bytes=self.max_image_fragments * IMAGE_DATA)
        except Exception as e:
            return self.fail(f"Bild nicht lesbar: {e}")
        count = -(-len(jpeg) // IMAGE_DATA)
        if count > self.max_image_fragments:
            return self.fail(f"Bild zu gross: {count} Pakete, erlaubt {self.max_image_fragments}")
        img_id = secrets.randbits(16)
        frags = [image_fragment(img_id, jpeg, i, count) for i in range(count)]
        entry = dict(time=datetime.now(), dest=dest, jpeg=jpeg, size=size, quality=q, fragments=count, sent=0,
                     resent=0, requests={})
        with self._lock:
            self.sent_images[img_id] = entry
            while len(self.sent_images) > MAX_IMAGES:
                self.sent_images.popitem(last=False)
        threading.Thread(target=self._send_fragments, args=(frags, dest, IMAGE_PORT, entry), daemon=True).start()
        return self.success(id=img_id, bytes=len(jpeg), fragments=count, width=size[0], height=size[1], quality=q,
                            seconds=round(count * max(self.fragment_delay, 0.1)))

    def execute_send_image(self, data):
        """ data = Bild als Base64 oder Data-URL """
        raw = data.get('data')
        if not raw:
            return self.fail("Parameter data fehlt (Bild als Base64)")
        if isinstance(raw, str):
            if raw.startswith('data:') and ',' in raw:
                raw = raw.split(',', 1)[1]
            try:
                raw = base64.b64decode(raw)
            except ValueError as e:
                return self.fail(f"data ist kein Base64: {e}")
        q = data.get('quality')
        return self.send_image(raw, dest=data.get('dest') or '^all', quality=int(q) if q else None)

    def receive_image_fragment(self, packet):
        payload = packet['decoded'].get('payload', b'')
        if not payload.startswith(IMAGE_MAGIC):
            return  # andere private App auf Port 256
        payload = payload[len(IMAGE_MAGIC):]
        if payload[:1] == bytes([IMAGE_RESEND]):
            return self.handle_resend_request(packet, payload)
        if len(payload) < IMAGE_HEADER.size:
            return
        ver, img_id, idx, count = IMAGE_HEADER.unpack(payload[:IMAGE_HEADER.size])
        if ver != IMAGE_VERSION or count == 0 or idx >= count or count > self.max_image_fragments:
            self.info(f"Bild-Paket verworfen: ver={ver} idx={idx} count={count}")
            return
        sender = packet.get('from', packet.get('fromId'))
        with self._lock:
            entry = self.received_images.get((sender, img_id))
            if entry is None or entry['count'] != count:
                entry = dict(time=datetime.now(), src=packet.get('fromId'), sender=sender, id=img_id, count=count,
                             parts={}, done=False, state=None, rounds=0, requested=0)
                self.received_images[(sender, img_id)] = entry
                while len(self.received_images) > MAX_IMAGES:
                    self.received_images.popitem(last=False)
            entry['parts'][idx] = payload[IMAGE_HEADER.size:]
            entry['last'] = datetime.now()
            entry['last_ts'] = time.time()
            complete = not entry['done'] and len(entry['parts']) == count
            if complete:
                entry['done'] = True
        if complete:
            self.emit_event('on_image', src=entry['src'], id=img_id, bytes=len(self.image_data(entry)))
        else:
            self._start_image_watch()

    # Nachforderung: Der Empfaenger prueft unvollstaendige Bilder, sobald image_resend_timeout lang kein Paket
    # kam. Ist der lueckenlose Anfang kleiner als image_min_complete, fordert er die fehlenden Pakete beim
    # Sender an (hoechstens image_max_rounds mal), sonst gibt er sich mit der Vorschau zufrieden.

    def _start_image_watch(self):
        with self._lock:
            if self._image_watch_running:
                return
            self._image_watch_running = True
        threading.Thread(target=self._image_watch, daemon=True).start()

    def _image_watch(self):
        while True:
            time.sleep(max(0.05, min(1.0, self.image_resend_timeout / 4)))
            now = time.time()
            with self._lock:
                pending = [e for e in self.received_images.values() if not e['done'] and e['state'] is None]
                if not pending:
                    self._image_watch_running = False
                    return
                due = [e for e in pending if now - e['last_ts'] >= self.image_resend_timeout]
            for entry in due:
                self.check_image(entry)

    def check_image(self, entry):
        """ unvollstaendiges Bild bewerten und bei zu vielen Verlusten fehlende Pakete nachfordern """
        with self._lock:
            count, parts = entry['count'], entry['parts']
            usable = self.contiguous_parts(entry) / count
            if entry['done'] or entry['state'] is not None:
                return
            if usable >= self.image_min_complete:
                entry['state'] = 'accepted'
                return
            if entry['rounds'] >= self.image_max_rounds:
                entry['state'] = 'given_up'
                return
            ranges = to_ranges(i for i in range(count) if i not in parts)[:MAX_RESEND_RANGES]
            entry['rounds'] += 1
            entry['requested'] += sum(n for _, n in ranges)
            entry['last_ts'] = time.time()       # naechste Pruefung erst nach erneutem Timeout
        request = IMAGE_MAGIC + RESEND_HEADER.pack(IMAGE_RESEND, entry['id']) + b''.join(RESEND_RANGE.pack(*r) for r in ranges)
        self.info(f"Bild {entry['id']} von {entry['src']}: {int(usable * 100)} % nutzbar, fordere "
                  f"{sum(n for _, n in ranges)} Pakete nach (Runde {entry['rounds']})")
        try:
            self.iface.sendData(request, destinationId=entry['sender'], portNum=IMAGE_PORT)
        except Exception as e:
            self.error(f"Nachforderung fehlgeschlagen: {e}")

    def handle_resend_request(self, packet, payload: bytes):
        """ Sender: angeforderte Pakete eines gesendeten Bildes erneut an den Anfrager senden """
        if len(payload) < RESEND_HEADER.size:
            return
        _, img_id = RESEND_HEADER.unpack(payload[:RESEND_HEADER.size])
        requester = packet.get('from', packet.get('fromId'))
        body = payload[RESEND_HEADER.size:]
        with self._lock:
            entry = self.sent_images.get(img_id)
            if entry is None:
                self.info(f"Nachforderung für unbekanntes Bild {img_id} von {packet.get('fromId')}")
                return
            rounds = entry['requests'].get(requester, 0)
            if rounds >= self.image_max_rounds:
                return
            entry['requests'][requester] = rounds + 1
            count = entry['fragments']
            indices = []
            for i in range(0, len(body) - RESEND_RANGE.size + 1, RESEND_RANGE.size):
                start, n = RESEND_RANGE.unpack(body[i:i + RESEND_RANGE.size])
                indices.extend(range(start, min(start + n, count)))
            indices = sorted(set(indices))[:count]
            entry['resent'] += len(indices)
            frags = [image_fragment(img_id, entry['jpeg'], i, count) for i in indices]
        self.info(f"Sende {len(frags)} Pakete von Bild {img_id} erneut an {packet.get('fromId')}")
        threading.Thread(target=self._send_fragments, args=(frags, requester, IMAGE_PORT), daemon=True).start()

    @staticmethod
    def contiguous_parts(entry) -> int:
        """ Anzahl der lueckenlos ab Paket 0 empfangenen Pakete """
        i = 0
        while i in entry['parts']:
            i += 1
        return i

    @classmethod
    def image_data(cls, entry) -> bytes:
        """ zusammenhaengender Anfang der empfangenen Pakete (nur der ist fuer progressive JPEG nutzbar) """
        return b''.join(entry['parts'][i] for i in range(cls.contiguous_parts(entry)))

    def receive_fragment(self, packet):
        payload = packet['decoded'].get('payload', b'')
        if not payload.startswith(JOB_MAGIC):
            return
        payload = payload[len(JOB_MAGIC):]
        if len(payload) < FRAGMENT_HEADER.size:
            return
        ver, msg_id, idx, count = FRAGMENT_HEADER.unpack(payload[:FRAGMENT_HEADER.size])
        if ver != PROTO_VERSION or count == 0 or idx >= count or count > self.max_fragments:
            self.info(f"Job-Paket verworfen: ver={ver} idx={idx} count={count}")
            return
        sender = packet.get('from', packet.get('fromId'))
        now = time.time()
        with self._lock:
            for key in [k for k, v in self._partial.items() if now - v['time'] > FRAGMENT_TIMEOUT]:
                del self._partial[key]
            entry = self._partial.setdefault((sender, msg_id), dict(time=now, count=count, parts={}))
            if entry['count'] != count:
                del self._partial[(sender, msg_id)]
                return
            entry['parts'][idx] = payload[FRAGMENT_HEADER.size:]
            if len(entry['parts']) < count:
                return
            del self._partial[(sender, msg_id)]
        token = b''.join(entry['parts'][i] for i in range(count))
        threading.Thread(target=self.handle_message, args=(token, sender, packet.get('fromId'), msg_id),
                         daemon=True).start()

    def handle_message(self, token: bytes, sender, from_id, msg_id: int):
        """ vollstaendige Nachricht entschluesseln, Job ausfuehren und das Ergebnis zuruecksenden """
        digest = hashlib.sha256(token).digest()
        now = time.time()
        with self._lock:
            ttl = float(self.max_age or FRAGMENT_TIMEOUT)
            for key in [k for k, t in self._seen.items() if now - t > ttl]:
                del self._seen[key]
            if digest in self._seen:
                self.info(f"Wiederholte Nachricht von {from_id} verworfen")
                return
            self._seen[digest] = now
        try:
            job = self.decode_message(token)
        except Exception as e:
            self.error(f"Job von {from_id} verworfen: {e}")
            return
        is_reply = job.get('type') == self.type and job.get('op') == 'd'
        try:
            if is_reply:
                result = self.execute(dict(job, src=from_id))
            else:
                result = self.getRoot().execute(job)
        except Exception as e:
            result = self.fail(f"{type(e).__name__}: {e}")
        if not isinstance(result, dict):
            result = dict(result=result)
        self.received_jobs.appendleft(dict(time=datetime.now(), src=from_id, id=msg_id, job=job,
                                           success=result.get('success')))
        if is_reply:
            return  # auf Ergebnisse wird nicht geantwortet
        reply = self.send_job(dict(type=self.type, op='d', ref=msg_id, r=result), dest=sender, ref=msg_id)
        if not reply.get('success') and reply.get('fragments'):
            self.send_job(dict(type=self.type, op='d', ref=msg_id, r=self.fail(reply['message'])),
                          dest=sender, ref=msg_id)

    def execute_send(self, data):
        job = data.get('job')
        if isinstance(job, str):
            try:
                job = json.loads(job)
            except ValueError as e:
                return self.fail(f"job ist kein gültiges JSON: {e}")
        if not isinstance(job, dict):
            return self.fail("Parameter job fehlt (JSON-Objekt)")
        return self.send_job(job, dest=data.get('dest') or '^all')

    def execute_d(self, data):
        """ Ergebnis eines entfernt ausgefuehrten Jobs: ref = ID des gesendeten Jobs, r = Ergebnis """
        ref, result, src = data.get('ref'), data.get('r'), data.get('src')
        with self._lock:
            entry = self.sent_jobs.get(ref)
            if entry is not None:
                entry['replies'].append(dict(time=datetime.now(), src=src, r=result))
        self.emit_event('on_result', ref=ref, src=src, r=result)
        return self.success()

    # -------------------------------------------------------------------------------------------------

    def _connect(self, target, factory):
        from pubsub import pub
        self.target = target
        self.connect_error = None
        try:
            self.iface = factory()
            pub.subscribe(self.onReceive, "meshtastic.receive")
            return self.success()
        except Exception as e:
            self.iface = None
            self.connect_error = str(e)
            self.error(f"Verbindung zu {target} fehlgeschlagen: {e}")
            return self.fail(str(e))

    def connect_serial(self, port='/dev/ttyACM0'):
        import meshtastic.serial_interface as serial_interface
        return self._connect(f"Serial {port}", lambda: serial_interface.SerialInterface(port))

    def connect_tcp(self, host):
        import meshtastic.tcp_interface as tcp
        return self._connect(f"TCP {host}", lambda: tcp.TCPInterface(hostname=host))

    def connect_ble(self, address):
        from meshtastic.ble_interface import BLEInterface
        return self._connect(f"BLE {address}", lambda: BLEInterface(address))

    def is_connected(self) -> bool:
        if self.iface is None:
            return False
        ev = getattr(self.iface, 'isConnected', None)
        return ev.is_set() if ev is not None else True

    def node_name(self, node_id) -> str:
        """ Kurz-/Langname eines Knotens aus der Node-DB, sonst die ID """
        try:
            user = (self.iface.nodes or {}).get(node_id, {}).get('user', {})
            if user:
                return f"{user.get('longName', '')} ({user.get('shortName', '')})"
        except Exception:
            pass
        return str(node_id)

    def my_node_name(self) -> str:
        try:
            info = self.iface.getMyNodeInfo() or {}
            user = info.get('user', {})
            return f"{user.get('longName', '?')} ({user.get('id', '?')})"
        except Exception:
            return '?'

    def find_devices(self) -> list:
        """ Serielle Ports, an denen ein Meshtastic-Geraet erkannt wurde """
        from meshtastic import util
        return list(util.findPorts(eliminate_duplicates=True))

    def find_tcp_devices(self, timeout: float = 3.0) -> list:
        """ Meshtastic-Knoten im Netz per mDNS (_meshtastic._tcp) """
        from zeroconf import Zeroconf, ServiceBrowser, ServiceListener
        names = []

        class Listener(ServiceListener):
            def add_service(self, zc, type_, name):
                names.append(name)

            def update_service(self, zc, type_, name):
                pass

            def remove_service(self, zc, type_, name):
                pass

        zc = Zeroconf()
        try:
            browser = ServiceBrowser(zc, MDNS_SERVICE, Listener())
            time.sleep(timeout)
            browser.cancel()
            result = []
            for name in dict.fromkeys(names):
                info = zc.get_service_info(MDNS_SERVICE, name, timeout=1000)
                if info is None or not info.parsed_addresses():
                    continue
                result.append(dict(name=name.replace('.' + MDNS_SERVICE, ''),
                                   host=info.parsed_addresses()[0], port=info.port))
            return result
        finally:
            zc.close()

    def find_ble_devices(self) -> list:
        """ Meshtastic-Geraete per Bluetooth LE (Scan dauert ca. 10 s) """
        from meshtastic.ble_interface import BLEInterface
        return [dict(name=d.name, address=d.address) for d in BLEInterface.scan()]

    def execute(self, data):
        if 'op' in data:
            return super().execute(data)
        if 'message' in data:
            return self.sendText(data['message'])
        if 'connect_serial' in data:
            return self.connect_serial(data['connect_serial'])
        if 'connect_tcp' in data:
            return self.connect_tcp(data['connect_tcp'])
        if 'connect_ble' in data:
            return self.connect_ble(data['connect_ble'])
        return super().execute(data)

    # --- UI ------------------------------------------------------------------------------------------

    def nav(self, p: base.Page, active=None):
        """ obere Navigation, active = Titel der aktuellen Seite """
        p.nav(("Übersicht", self.link(self.part_index), "Verbindungsstatus und Nachrichten"),
              ("Messages", self.link(self.part_messages), f"Die letzten {MAX_MESSAGES} empfangenen Nachrichten"),
              ("Jobs", self.link(self.part_jobs), f"Verschlüsselte Jobs über Port {JOB_PORT}"),
              ("Bilder", self.link(self.part_images), f"Bilder über Port {IMAGE_PORT} senden und empfangen"),
              ("Devices", self.link(self.part_devices), "Verfügbare Meshtastic-Geräte"),
              ("GPIO", self.link(self.part_gpios), "Remote Hardware"),
              active=active)

    def part_status(self, p: base.Page):
        if self.is_connected():
            p.prop("Verbindung", w.badge("verbunden", kind='success') + ' ' + w.s(self.target))
            p.prop("Knoten", w.s(self.my_node_name()))
        elif self.iface is not None:
            p.prop("Verbindung", w.badge("verbindet...", kind='warn') + ' ' + w.s(self.target))
        elif self.connect_error is not None:
            p.prop("Verbindung", w.badge("Fehler", kind='error') + ' ' + w.s(self.target))
            p(w.alert_error(self.connect_error, escape=True))
        else:
            p.prop("Verbindung", w.badge("nicht verbunden", kind='default'))

    def part_messages(self, p: base.Page, params={}):
        self.nav(p, "Messages")
        with p.section(h=f"Letzte {MAX_MESSAGES} Nachrichten"):
            if not self.messages:
                p(w.alert_info("Noch keine Nachrichten empfangen."))
                return
            rows = [['Zeit', 'Von', 'Nachricht', 'SNR', 'RSSI']]
            for m in list(self.messages):
                rows.append([m['time'].strftime('%d.%m. %H:%M:%S'), w.s(m['sender']), w.s(m['text']),
                             '' if m['snr'] is None else m['snr'], '' if m['rssi'] is None else m['rssi']])
            p(w.table(rows))

    @staticmethod
    def short_json(value, limit=120) -> str:
        text = json.dumps(value, default=str, ensure_ascii=False)
        return w.s(text if len(text) <= limit else text[:limit] + '…')

    def part_jobs(self, p: base.Page, params={}):
        self.nav(p, "Jobs")
        with p.section(h="Job senden"):
            if not self.has_crypt():
                p(w.alert_warn("Kein CryptRunner (type crypt) eingerichtet, Jobs können nicht verschlüsselt werden."))
            p.form_input('dest', "Ziel (!id oder ^all)", id='mb_dest', value='^all')
            p.form_input('job', "Job (JSON)", id='mb_job', value='{"type": "ping"}')
            p(self.action_btn_parametric("Senden", dict(type=self.type, op='send', dest='#mb_dest', job='#mb_job')))
        with p.section(h="Gesendete Jobs"):
            with self._lock:
                sent = list(self.sent_jobs.items())[::-1]
            if not sent:
                p(w.alert_info("Noch keine Jobs gesendet."))
            else:
                rows = [['Zeit', 'ID', 'Ziel', 'Job', 'Pakete', 'Ergebnis']]
                for msg_id, e in sent:
                    res = '<br>'.join(f"{w.s(rp['src'])}: {self.short_json(rp['r'])}" for rp in e['replies'])
                    rows.append([e['time'].strftime('%d.%m. %H:%M:%S'), msg_id, w.s(e['dest']),
                                 self.short_json(e['job']), f"{e['fragments']} ({e['size']} B)",
                                 res or w.badge("wartet", kind='warn')])
                p(w.table(rows))
        with p.section(h="Empfangene Jobs"):
            if not self.received_jobs:
                p(w.alert_info("Noch keine Jobs empfangen."))
            else:
                rows = [['Zeit', 'Von', 'ID', 'Job', 'Erfolg']]
                for e in list(self.received_jobs):
                    rows.append([e['time'].strftime('%d.%m. %H:%M:%S'), w.s(e['src']), e['id'],
                                 self.short_json(e['job']), w.s(e['success'])])
                p(w.table(rows))

    # Dateiauswahl als Ergaenzung zur Dropzone (Handy: Galerie oder Kamera), schreibt Base64 in #image_data
    JS_IMAGE_PICK = """
        function mb_image_pick(input) {
            const file = input.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = () => {
                document.getElementById('image_data').value = String(reader.result).split(',', 2)[1] || '';
                document.getElementById('mb_image_name').textContent = file.name;
            };
            reader.readAsDataURL(file);
        }
    """

    @staticmethod
    def img_tag(jpeg: bytes, **attrs) -> str:
        style = attrs.pop('style', 'max-width: 100%;')
        return f'<img src="data:image/jpeg;base64,{base64.b64encode(jpeg).decode("ascii")}" style="{style}">'

    def part_images(self, p: base.Page, params={}):
        """ Formular nach ImageExecutor.ui_imageform: Dropzone fuellt das versteckte Feld image_data """
        self.nav(p, "Bilder")
        with p.section(h="Bild senden"):
            p.div(f"Das Bild wird auf {IMAGE_MAX_EDGE} px (längste Kante) verkleinert, als progressive JPEG "
                  f"stark komprimiert und in Paketen zu {IMAGE_DATA} Byte über Port {IMAGE_PORT} gesendet.")
            p.js_ready('nx_initFileDragArea("dropZone", "image_data");')
            p.dropzone('dropZone', "Bild hier ablegen")
            p(f'<div><input type="file" accept="image/*" onchange="mb_image_pick(this)"> '
              f'<span id="mb_image_name"></span></div>')
            p.script(self.JS_IMAGE_PICK)
            p.hidden('image_data', '', 'image_data')
            p.form_input('dest', "Ziel (!id oder ^all)", id='mb_image_dest', value='^all')
            p.form_input('quality', "JPEG-Qualität", id='mb_image_quality', value=str(self.image_quality))
            p.form_elem(self.action_btn_parametric("Senden", dict(
                type=self.type, op='send_image', data='#image_data', dest='#mb_image_dest',
                quality='#mb_image_quality')))
        active = False
        with p.section(h="Empfangene Bilder"):
            with self._lock:
                received = [dict(e, parts=dict(e['parts'])) for e in self.received_images.values()][::-1]
            if not received:
                p(w.alert_info("Noch keine Bilder empfangen."))
            for e in received:
                data = self.image_data(e)
                n = self.contiguous_parts(e)
                active = active or (not e['done'] and e['state'] is None)
                if e['done']:
                    state = w.badge("vollständig", kind='success')
                elif e['state'] == 'accepted':
                    state = w.badge(f"ausreichend ({int(100 * n / e['count'])} % nutzbar)", kind='info')
                elif e['state'] == 'given_up':
                    state = w.badge(f"unvollständig ({int(100 * n / e['count'])} % nutzbar)", kind='error')
                else:
                    state = w.badge(f"{len(e['parts'])}/{e['count']} Pakete", kind='warn')
                    if n < len(e['parts']):
                        state += f" – Paket {n + 1} fehlt, Vorschau nur bis dahin"
                if e['rounds']:
                    state += f" – {e['rounds']}× nachgefordert ({e['requested']} Pakete)"
                p.div(f"{e['time'].strftime('%d.%m. %H:%M:%S')} von {w.s(e['src'])} {state}")
                shown = data if e['done'] else (preview_jpeg(data) if data else None)
                if shown:
                    p(self.img_tag(shown))
        with p.section(h="Gesendete Bilder"):
            with self._lock:
                sent = list(self.sent_images.items())[::-1]
            if not sent:
                p(w.alert_info("Noch keine Bilder gesendet."))
            for img_id, e in sent:
                active = active or e['sent'] < e['fragments']
                state = w.badge("Fehler", kind='error') if e.get('error') else \
                    w.badge(f"{e['sent']}/{e['fragments']} Pakete", kind='success' if e['sent'] == e['fragments'] else 'warn')
                p.div(f"{e['time'].strftime('%d.%m. %H:%M:%S')} an {w.s(e['dest'])} – {e['size'][0]}×{e['size'][1]} px, "
                      f"Qualität {e['quality']}, {len(e['jpeg'])} Byte {state}"
                      + (f" – {e['resent']} Pakete nachgesendet" if e['resent'] else ''))
                p(self.img_tag(e['jpeg'], style='max-width: 300px;'))
        if active:
            p.script("setTimeout(() => location.reload(), 10000);")

    def connect_btn(self, op: str, target: str):
        return self.action_btn({'type': self.type, op: target, 'title': "Verbinden"}, hint=f"Mit {target} verbinden")

    def _device_list(self, p: base.Page, finder, empty_msg: str, header: list, row):
        try:
            devices = finder()
        except Exception as e:
            p(w.alert_error(f"Suche fehlgeschlagen: {e}", escape=True))
            return
        if not devices:
            p(w.alert_info(empty_msg))
            return
        p(w.table([header] + [row(d) for d in devices]))

    def part_devices(self, p: base.Page, params={}):
        self.nav(p, "Devices")
        scan = params.get('scan')
        with p.section(h="Serial"):
            self._device_list(p, self.find_devices, "Kein Meshtastic-Gerät an einem seriellen Port gefunden.",
                              ['Port', ''], lambda port: [w.s(port), self.connect_btn('connect_serial', port)])
        with p.section(h="TCP (mDNS)"):
            if scan == 'tcp':
                self._device_list(p, self.find_tcp_devices, "Kein Meshtastic-Knoten im Netz gefunden.",
                                  ['Name', 'Host', 'Port', ''],
                                  lambda d: [w.s(d['name']), w.s(d['host']), d['port'],
                                             self.connect_btn('connect_tcp', d['host'])])
            p(w.a("Netzwerk durchsuchen", self.link(self.part_devices, scan='tcp')))
        with p.section(h="Bluetooth LE"):
            if scan == 'ble':
                self._device_list(p, self.find_ble_devices, "Kein Meshtastic-Gerät per BLE gefunden.",
                                  ['Name', 'Adresse', ''],
                                  lambda d: [w.s(d['name']), w.s(d['address']),
                                             self.connect_btn('connect_ble', d['address'])])
            p(w.a("BLE scannen (ca. 10 s)", self.link(self.part_devices, scan='ble')))

    def part_gpios(self, p: base.Page, params={}):
        self.nav(p, "GPIO")
        p("GPIOs")
        from meshtastic.remote_hardware import RemoteHardwareClient
        # meshtastic/remote_hardware.

    def part_index(self, p: base.Page, params={}):
        self.nav(p, "Übersicht")
        with p.section(h="Meshtastic"):
            self.part_status(p)
            p.prop("Receive Count", self.receive_count)
            p.form_input('message', "Message", id='message')
            p(self.action_btn_parametric("Send", dict(type=self.type, message='#message')))
            p(w.s(self.last))
        with p.section(h="Meshtastic"):
            p("Io25 ist die grüne blink led")
