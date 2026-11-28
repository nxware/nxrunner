import os
import random
import threading
import time

from nxrunner.network.rs485_proto import (cobs_encode, cobs_decode, Frame, FrameReader, ArqLink, BusMaster,
                                          BusSlave, BusTiming, DATA, FIRST, LAST, END)


class SimPort:
    """ Ende eines simulierten Busses: verfaelscht und verliert Bytes mit der Wahrscheinlichkeit error """

    def __init__(self, error=0.0, seed=1):
        self.peer = None
        self.buf = bytearray()
        self.cond = threading.Condition()
        self.error = error
        self.rnd = random.Random(seed)

    def write(self, data):
        out = bytearray()
        for b in data:
            x = self.rnd.random()
            if x < self.error / 2:
                continue  # Byte verloren
            out.append(b ^ (1 << self.rnd.randrange(8)) if x < self.error else b)
        with self.peer.cond:
            self.peer.buf += out
            self.peer.cond.notify_all()

    def read(self, timeout):
        with self.cond:
            if not self.buf:
                self.cond.wait(max(0.0, timeout))
            data, self.buf = bytes(self.buf), bytearray()
            return data


def sim_pair(error):
    a, b = SimPort(error, 1), SimPort(error, 2)
    a.peer, b.peer = b, a
    return a, b


def test_cobs_roundtrip():
    rnd = random.Random(3)
    for n in (0, 1, 253, 254, 255, 600):
        data = bytes(rnd.choice([0, 0, rnd.randrange(256)]) for _ in range(n))
        enc = cobs_encode(data)
        assert 0 not in enc
        assert cobs_decode(enc) == data


def test_frame_crc_detects_corruption():
    f = Frame(2, 1, DATA | FIRST | LAST | END, 7, 100, 99, 5, 0b101, b'hallo\x00welt')
    raw = f.encode()
    assert raw.endswith(b'\0')
    g = Frame.decode(raw[:-1])
    assert (g.dst, g.src, g.seq, g.base, g.ack, g.sack, g.payload) == (2, 1, 100, 99, 5, 0b101, b'hallo\x00welt')
    bad = bytearray(raw[:-1])
    bad[5] ^= 0x10
    assert Frame.decode(bytes(bad)) is None


def test_reader_resyncs_after_noise():
    reader = FrameReader(None)
    f = Frame(2, 1, DATA, 1, payload=b'x' * 50).encode()
    frames = reader.feed(b'\x13\x37garbage\x00' + f[:20] + b'\x00' + f + f)
    assert len(frames) == 2 and reader.bad == 2


def test_sender_drop_resyncs_receiver():
    got = []
    tx, rx = ArqLink(2, None, max_tries=2, frag=24), ArqLink(1, got.append)
    tx.enqueue(b'A' * 60)
    frames = tx.build_turn(1)
    rx.on_frame(frames[0])  # nur das erste Fragment von A kommt an, ack geht verloren
    tx.build_turn(1)
    tx.enqueue(b'B' * 30)
    frames = tx.build_turn(1)  # A ueberschreitet max_tries und wird verworfen, B ist neu
    for f in frames:
        rx.on_frame(f)
    assert got == [b'B' * 30]
    assert tx.stats.drop_tries == 1


def run_bus(error, packets=25, timeout=60):
    port_m, port_s = sim_pair(error)
    timing = BusTiming(115200, reply_ms=30)
    got_m, got_s = [], []
    link_m = ArqLink(2, got_m.append, max_tries=200)
    link_s = ArqLink(1, got_s.append, max_tries=200)
    master, slave = BusMaster(port_m, {2: link_m}, timing, idle_poll=0.005), BusSlave(port_s, link_s, 2, timing)
    threads = [threading.Thread(target=x.run, daemon=True) for x in (master, slave)]
    for t in threads:
        t.start()
    rnd = random.Random(5)
    sent_m = [os.urandom(rnd.randint(20, 1500)) for _ in range(packets)]
    sent_s = [os.urandom(rnd.randint(20, 1500)) for _ in range(packets)]
    for a, b in zip(sent_m, sent_s):
        assert link_m.enqueue(a) and link_s.enqueue(b)
    end = time.time() + timeout
    while time.time() < end and (len(got_m) < packets or len(got_s) < packets):
        time.sleep(0.05)
    master.stop.set()
    slave.stop.set()
    return sent_m, sent_s, got_m, got_s, link_m, master.reader


def test_bus_clean_line():
    sent_m, sent_s, got_m, got_s, link, _ = run_bus(0.0)
    assert got_s == sent_m and got_m == sent_s


def test_bus_bad_line_delivers_everything_in_order():
    """ 0,3 % der Bytes kaputt oder verloren: ~30 % der Rahmen defekt, trotzdem kommt alles an """
    sent_m, sent_s, got_m, got_s, link, reader = run_bus(0.003)
    assert got_s == sent_m and got_m == sent_s
    assert reader.bad > 0 and link.stats.retransmits > 0
