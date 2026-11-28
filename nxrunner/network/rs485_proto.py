"""
Halbduplex-Protokoll fuer IP ueber 2-Draht-RS-485 (auch fuer schlechte Leitungen)

Auf einem RS-485-Paar darf immer nur einer senden. Deshalb gilt Master/Slave-Polling:

  - Der Master (Adresse 1) beginnt jeden Turn: er sendet bis zu `window` Fragmente an einen Slave
    (oder nur einen leeren Rahmen) und markiert den letzten Rahmen mit END.
  - Der Slave antwortet nur nach einem END an ihn, nach einer kurzen Umschaltpause (guard), ebenfalls
    mit bis zu `window` Fragmenten und END. Danach ist der naechste Slave dran.

Robustheit fuer gestoerte Leitungen:

  - Rahmen mit COBS-Kodierung und 0x00 als Trenner: nach einem Bitfehler synchronisiert sich der Empfaenger
    sofort am naechsten Trenner wieder.
  - CRC32 je Rahmen, defekte Rahmen werden verworfen.
  - IP-Pakete werden in kleine Fragmente zerlegt, ein Fehler kostet nur ein Fragment.
  - Selektive Wiederholung (kumulatives ack + 32-Bit-SACK in jedem Rahmen).
  - Die Fragmentgroesse passt sich der Wiederholrate an (schlechte Leitung -> kleinere Fragmente).
  - Nach max_tries Versuchen wird ein Paket verworfen (TCP wiederholt selbst), der Empfaenger springt ueber `base`
    weiter und setzt die Zusammensetzung am naechsten FIRST-Fragment neu auf.

Rahmen (vor COBS): dst(1) src(1) flags(1) sess(1) seq(2) base(2) ack(2) sack(4) payload(n) crc32(4)
"""
import collections
import random
import struct
import threading
import time
import zlib

MASTER_ADDR = 1

DATA, FIRST, LAST, END = 1, 2, 4, 8
SACK_BITS = 32
MAX_PACKET = 65535


def seq_diff(a, b) -> int:
    """ a - b modulo 2^16 (0..65535), Werte >= 0x8000 bedeuten 'a liegt vor b' """
    return (a - b) & 0xFFFF


def seq_before(a, b) -> bool:
    return seq_diff(a, b) >= 0x8000


# ----------------------------------------------------------------------
# COBS
# ----------------------------------------------------------------------

def cobs_encode(data: bytes) -> bytes:
    out = bytearray([0])
    code_idx, code = 0, 1
    for b in data:
        if b == 0:
            out[code_idx] = code
            code_idx, code = len(out), 1
            out.append(0)
        else:
            out.append(b)
            code += 1
            if code == 0xFF:
                out[code_idx] = code
                code_idx, code = len(out), 1
                out.append(0)
    out[code_idx] = code
    return bytes(out)


def cobs_decode(data: bytes) -> bytes:
    out = bytearray()
    i, n = 0, len(data)
    while i < n:
        code = data[i]
        if code == 0 or i + code > n:
            raise ValueError("COBS fehlerhaft")
        out += data[i + 1:i + code]
        i += code
        if code < 0xFF and i < n:
            out.append(0)
    return bytes(out)


# ----------------------------------------------------------------------
# Rahmen
# ----------------------------------------------------------------------

class Frame:
    HDR = struct.Struct('>BBBBHHHI')
    __slots__ = ('dst', 'src', 'flags', 'sess', 'seq', 'base', 'ack', 'sack', 'payload')

    def __init__(self, dst, src, flags, sess, seq=0, base=0, ack=0, sack=0, payload=b''):
        self.dst, self.src, self.flags, self.sess = dst, src, flags, sess
        self.seq, self.base, self.ack, self.sack, self.payload = seq, base, ack, sack, payload

    def encode(self) -> bytes:
        body = self.HDR.pack(self.dst, self.src, self.flags, self.sess, self.seq, self.base, self.ack,
                             self.sack) + self.payload
        return cobs_encode(body + struct.pack('>I', zlib.crc32(body) & 0xFFFFFFFF)) + b'\0'

    @classmethod
    def decode(cls, raw: bytes):
        """ :return: Frame oder None bei COBS-/CRC-Fehler """
        try:
            data = cobs_decode(raw)
        except ValueError:
            return None
        if len(data) < cls.HDR.size + 4:
            return None
        body, crc = data[:-4], struct.unpack('>I', data[-4:])[0]
        if zlib.crc32(body) & 0xFFFFFFFF != crc:
            return None
        return cls(*cls.HDR.unpack(body[:cls.HDR.size]), payload=body[cls.HDR.size:])

    def __repr__(self):
        return f"Frame({self.src}->{self.dst} f={self.flags} seq={self.seq} ack={self.ack} len={len(self.payload)})"


class FrameReader:
    """ Zerlegt den Bytestrom am 0x00-Trenner in Rahmen und zaehlt defekte Rahmen """

    MAX_BUF = 4096

    def __init__(self, port):
        self.port = port
        self.buf = bytearray()
        self.ok = 0
        self.bad = 0

    def feed(self, data: bytes) -> list:
        self.buf += data
        frames = []
        while True:
            i = self.buf.find(0)
            if i < 0:
                break
            raw = bytes(self.buf[:i])
            del self.buf[:i + 1]
            if not raw:
                continue
            frame = Frame.decode(raw)
            if frame is None:
                self.bad += 1
            else:
                self.ok += 1
                frames.append(frame)
        if len(self.buf) > self.MAX_BUF:  # Rauschen ohne Trenner
            self.buf.clear()
            self.bad += 1
        return frames

    def poll(self, timeout) -> list:
        return self.feed(self.port.read(timeout))

    def read_turn(self, src, dst, first_timeout, idle_gap) -> list:
        """ Rahmen von src an dst bis END, Ende auch bei Stille (idle_gap) nach den letzten Bytes """
        frames = []
        deadline = time.monotonic() + first_timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return frames
            data = self.port.read(remaining)
            if data:
                deadline = max(deadline, time.monotonic() + idle_gap)
            for f in self.feed(data):
                if f.src == src and f.dst == dst:
                    frames.append(f)
                    if f.flags & END:
                        return frames

    def error_rate(self) -> float:
        total = self.ok + self.bad
        return self.bad / total if total else 0.0


# ----------------------------------------------------------------------
# Sicherungsschicht je Gegenstelle
# ----------------------------------------------------------------------

class Fragment:
    __slots__ = ('seq', 'pkt', 'data', 'flags', 'tries')

    def __init__(self, seq, pkt, data, flags):
        self.seq, self.pkt, self.data, self.flags, self.tries = seq, pkt, data, flags, 0


class LinkStats:
    KEYS = ('tx_frames', 'rx_frames', 'tx_packets', 'rx_packets', 'tx_bytes', 'rx_bytes', 'retransmits',
            'drop_queue', 'drop_tries', 'duplicates', 'turns', 'missed_turns')

    def __init__(self):
        for key in self.KEYS:
            setattr(self, key, 0)

    def to_dict(self) -> dict:
        return {key: getattr(self, key) for key in self.KEYS}


class ArqLink:
    """
    Selektive Wiederholung zu einer Gegenstelle. Sender: Fragmente in inflight (seq-sortiert), Empfaenger:
    rx_expected (kumulativ), rx_ooo (vorab empfangene Fragmente), rx_partial (Paket im Aufbau).
    """

    def __init__(self, peer, deliver, window=8, frag=96, frag_min=24, frag_max=240, max_tries=16, queue=32):
        self.peer, self.deliver = peer, deliver
        self.window = max(1, min(int(window), SACK_BITS))
        self.frag_min, self.frag_max = int(frag_min), min(int(frag_max), 240)
        self.frag = max(self.frag_min, min(int(frag), self.frag_max))
        self.max_tries, self.queue_max = int(max_tries), int(queue)
        self.sess = random.randint(1, 255)
        self.packets = collections.deque()
        self.lock = threading.Lock()
        self.inflight = collections.OrderedDict()
        self.next_seq = random.randint(0, 0xFFFF)
        self.base = self.next_seq
        self.tx_current = None  # [pkt_id, data, offset]
        self.pkt_counter = 0
        self.rx_sess, self.rx_expected, self.rx_ooo, self.rx_partial = None, 0, {}, None
        self.retx_avg = 0.0
        self.last_seen = 0.0
        self.stats = LinkStats()
        self.rate_tx = self.rate_rx = 0.0
        self.meter = (time.monotonic(), 0, 0)

    # --- Sender ---------------------------------------------------------

    def enqueue(self, packet: bytes) -> bool:
        with self.lock:
            if len(self.packets) >= self.queue_max:
                self.stats.drop_queue += 1  # Tail-Drop: alte Pakete zuerst, kein Puffer-Stau
                return False
            self.packets.append(packet)
            return True

    def has_pending(self) -> bool:
        return bool(self.inflight or self.packets or self.tx_current)

    def fill(self):
        """ erzeugt neue Fragmente, bis das Fenster voll ist """
        while seq_diff(self.next_seq, self.base) < self.window:
            if self.tx_current is None:
                with self.lock:
                    if not self.packets:
                        return
                    self.pkt_counter += 1
                    self.tx_current = [self.pkt_counter, self.packets.popleft(), 0]
            pkt, data, off = self.tx_current
            chunk = data[off:off + self.frag]
            flags = DATA | (FIRST if off == 0 else 0) | (LAST if off + len(chunk) >= len(data) else 0)
            self.inflight[self.next_seq] = Fragment(self.next_seq, pkt, chunk, flags)
            self.next_seq = (self.next_seq + 1) & 0xFFFF
            self.tx_current = None if flags & LAST else [pkt, data, off + len(chunk)]

    def drop_head_packet(self):
        """ verwirft das Paket des aeltesten Fragments nach zu vielen Versuchen """
        head = next(iter(self.inflight.values()))
        for seq in [s for s, f in self.inflight.items() if f.pkt == head.pkt]:
            del self.inflight[seq]
        if self.tx_current is not None and self.tx_current[0] == head.pkt:
            self.tx_current = None
        self.stats.drop_tries += 1
        self.retx_avg = 0.9 * self.retx_avg + 0.1 * self.max_tries
        self.update_base()

    def update_base(self):
        self.base = next(iter(self.inflight)) if self.inflight else self.next_seq

    def build_turn(self, me) -> list:
        """ Rahmen fuer einen Turn: alle unbestaetigten Fragmente im Fenster, letzter Rahmen mit END """
        while self.inflight and next(iter(self.inflight.values())).tries >= self.max_tries:
            self.drop_head_packet()
        self.fill()
        ack, sack = self.rx_expected, self.sack()
        frames = []
        for frag in self.inflight.values():
            frag.tries += 1
            self.stats.retransmits += frag.tries > 1
            frames.append(Frame(self.peer, me, frag.flags, self.sess, frag.seq, self.base, ack, sack, frag.data))
        if not frames:
            frames.append(Frame(self.peer, me, 0, self.sess, self.next_seq, self.base, ack, sack))
        frames[-1].flags |= END
        self.stats.tx_frames += len(frames)
        return frames

    def process_ack(self, ack, sack):
        if seq_diff(ack, self.base) > seq_diff(self.next_seq, self.base):
            return  # ausserhalb des Fensters (alter Rahmen)
        acked = [s for s in self.inflight if seq_before(s, ack)]
        acked += [(ack + 1 + i) & 0xFFFF for i in range(SACK_BITS) if sack >> i & 1]
        for seq in acked:
            frag = self.inflight.pop(seq, None)
            if frag is not None:
                self.retx_avg = 0.9 * self.retx_avg + 0.1 * (frag.tries - 1)
                if frag.flags & LAST:
                    self.stats.tx_packets += 1
                self.stats.tx_bytes += len(frag.data)
        self.update_base()

    # --- Empfaenger -----------------------------------------------------

    def sack(self) -> int:
        bits = 0
        for i in range(SACK_BITS):
            if (self.rx_expected + 1 + i) & 0xFFFF in self.rx_ooo:
                bits |= 1 << i
        return bits

    def on_frame(self, f: Frame):
        self.last_seen = time.monotonic()
        self.stats.rx_frames += 1
        if f.sess != self.rx_sess:  # Gegenstelle neu gestartet
            self.rx_sess, self.rx_expected, self.rx_ooo, self.rx_partial = f.sess, f.base, {}, None
        self.process_ack(f.ack, f.sack)
        if 0 < seq_diff(f.base, self.rx_expected) < 0x8000:  # Sender hat Pakete verworfen
            self.rx_expected, self.rx_partial = f.base, None
            self.rx_ooo = {s: v for s, v in self.rx_ooo.items() if not seq_before(s, f.base)}
            self.drain()
        if f.flags & DATA:
            self.rx_data(f)

    def rx_data(self, f: Frame):
        d = seq_diff(f.seq, self.rx_expected)
        if d == 0:
            self.accept(f)
            self.rx_expected = (self.rx_expected + 1) & 0xFFFF
            self.drain()
        elif d <= SACK_BITS and f.seq not in self.rx_ooo:
            self.rx_ooo[f.seq] = f
        else:
            self.stats.duplicates += 1

    def drain(self):
        while self.rx_expected in self.rx_ooo:
            self.accept(self.rx_ooo.pop(self.rx_expected))
            self.rx_expected = (self.rx_expected + 1) & 0xFFFF

    def accept(self, f: Frame):
        """ setzt Pakete in seq-Reihenfolge zusammen, ohne FIRST wird bis zum naechsten Paketanfang verworfen """
        if f.flags & FIRST:
            self.rx_partial = bytearray()
        if self.rx_partial is None:
            return
        self.rx_partial += f.payload
        if len(self.rx_partial) > MAX_PACKET:
            self.rx_partial = None
        elif f.flags & LAST:
            packet, self.rx_partial = bytes(self.rx_partial), None
            self.stats.rx_packets += 1
            self.stats.rx_bytes += len(packet)
            self.deliver(packet)

    # --- Turn-Ende: Anpassung und Messung -------------------------------

    def end_turn(self, answered: bool):
        self.stats.turns += 1
        self.stats.missed_turns += not answered
        if self.stats.turns % 10 == 0:
            self.adapt()
        now, t_bytes, r_bytes = time.monotonic(), self.stats.tx_bytes, self.stats.rx_bytes
        t0, t_last, r_last = self.meter
        if now - t0 >= 2:
            self.rate_tx, self.rate_rx = (t_bytes - t_last) / (now - t0), (r_bytes - r_last) / (now - t0)
            self.meter = (now, t_bytes, r_bytes)

    def adapt(self):
        """ Fragmentgroesse nach Wiederholrate: viele Wiederholungen -> kleiner, fehlerfrei -> groesser """
        if self.retx_avg > 0.5:
            self.frag = max(self.frag_min, int(self.frag * 0.75))
        elif self.retx_avg < 0.1:
            self.frag = min(self.frag_max, self.frag + 8)

    def alive(self, timeout=3.0) -> bool:
        return self.last_seen > 0 and time.monotonic() - self.last_seen < timeout

    def to_dict(self) -> dict:
        return dict(self.stats.to_dict(), peer=self.peer, alive=self.alive(), frag=self.frag,
                    retx_avg=round(self.retx_avg, 2), queue=len(self.packets), inflight=len(self.inflight),
                    last_seen=round(time.monotonic() - self.last_seen, 1) if self.last_seen else None,
                    rate_tx=round(self.rate_tx), rate_rx=round(self.rate_rx))


# ----------------------------------------------------------------------
# Buszugriff
# ----------------------------------------------------------------------

class BusTiming:
    """ Zeiten aus der Baudrate: guard = Umschaltpause vor dem Senden, gap = Stille, die einen Turn beendet """

    def __init__(self, baud, guard_ms=0, reply_ms=80):
        self.char = 10.0 / int(baud)
        self.guard = guard_ms / 1000.0 if guard_ms else max(0.002, 4 * self.char)
        self.reply = reply_ms / 1000.0
        self.gap = max(0.02, 40 * self.char)

    def tx_time(self, n) -> float:
        return n * self.char


class BusMaster:
    """ fragt die Slaves nacheinander ab, nur der Master beginnt einen Turn """

    def __init__(self, port, links: dict, timing: BusTiming, idle_poll=0.05):
        self.port, self.links, self.timing, self.idle_poll = port, links, timing, idle_poll
        self.reader = FrameReader(port)
        self.stop = threading.Event()
        self.wake = threading.Event()

    def run(self):
        while not self.stop.is_set():
            busy = False
            for link in list(self.links.values()):
                busy = self.turn(link) or busy
            if not busy:
                self.wake.wait(self.idle_poll)
                self.wake.clear()

    def turn(self, link: ArqLink) -> bool:
        """ :return: True wenn noch Daten unterwegs sind """
        data = b''.join(f.encode() for f in link.build_turn(MASTER_ADDR))
        self.port.write(data)
        reply = self.reader.read_turn(link.peer, MASTER_ADDR, self.timing.reply + self.timing.tx_time(len(data)),
                                      self.timing.gap)
        for f in reply:
            link.on_frame(f)
        link.end_turn(bool(reply))
        time.sleep(self.timing.guard)
        return link.has_pending() or any(f.flags & DATA for f in reply)


class BusSlave:
    """ antwortet nur, wenn der Master mit END an diese Adresse uebergibt """

    def __init__(self, port, link: ArqLink, address, timing: BusTiming):
        self.port, self.link, self.address, self.timing = port, link, int(address), timing
        self.reader = FrameReader(port)
        self.stop = threading.Event()
        self.wake = threading.Event()  # nicht benoetigt, gleiche Schnittstelle wie BusMaster

    def run(self):
        while not self.stop.is_set():
            for f in self.reader.poll(0.1):
                if f.src != MASTER_ADDR or f.dst != self.address:
                    continue
                self.link.on_frame(f)
                if f.flags & END:
                    self.answer()

    def answer(self):
        time.sleep(self.timing.guard)
        self.port.write(b''.join(f.encode() for f in self.link.build_turn(self.address)))
        self.link.end_turn(True)
