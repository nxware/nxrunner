"""
Unittests fuer nxrunner.devices.meshtastic (Jobs ueber Port 370), ohne Funkhardware

    python -m unittest tests.test_meshtastic
"""
import base64
import os
import tempfile
import threading
import time
import unittest

from nwebclient import runner as r
from nwebclient.runner import LazyDispatcher

from nxrunner.devices import meshtastic as m
from nxrunner.system.crypt import CryptRunner


class Recorder(r.BaseJobExecutor):
    def __init__(self, type='rec'):
        super().__init__(type)
        self.jobs = []

    def execute(self, data):
        self.jobs.append(dict(data))
        return self.success(value=data.get('x'), echo=data.get('pad', ''))


class FakeIface:
    """ Funkstrecke im Speicher: sendData liefert das Paket beim Partner ab """

    def __init__(self, num):
        self.num = num
        self.peer = None
        self.bot = None
        self.isConnected = threading.Event()
        self.isConnected.set()
        self.nodes = {}
        self.sent = []
        self.drop = False

    def sendData(self, data, destinationId='^all', portNum=None, **kw):
        assert len(data) <= m.MAX_PAYLOAD, len(data)
        self.sent.append(bytes(data))
        if self.drop:
            return
        packet = {'from': self.num, 'fromId': f'!{self.num:08x}', 'to': destinationId,
                  'decoded': {'portnum': portNum, 'payload': bytes(data)}}
        self.peer.bot.onReceive(packet, self.peer)


def make_key(path):
    from cryptography.fernet import Fernet
    with open(path, 'wb') as f:
        f.write(Fernet.generate_key())


def wait_for(cond, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.02)
    return False


class MeshJobTestCase(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.key_a = os.path.join(self.tmp, 'a.key')
        make_key(self.key_a)
        self.a, self.ia, self.rec_a = self.node(1, self.key_a)
        self.b, self.ib, self.rec_b = self.node(2, self.key_a)
        self.ia.peer, self.ib.peer = self.ib, self.ia

    def node(self, num, key):
        disp = LazyDispatcher()
        disp.add_runner(CryptRunner(key_file=key))
        rec = Recorder()
        disp.add_runner(rec)
        bot = m.MeshtasticBot(fragment_delay=0)
        disp.add_runner(bot)
        iface = FakeIface(num)
        iface.bot = bot
        bot.iface = iface
        return bot, iface, rec

    def test_type(self):
        self.assertEqual('mb', self.a.type)

    def test_roundtrip(self):
        res = self.a.execute(dict(type='mb', op='send', dest='!00000002', job='{"type": "rec", "x": 7}'))
        self.assertTrue(res['success'], res)
        self.assertEqual(1, res['fragments'])
        self.assertTrue(wait_for(lambda: self.a.sent_jobs[res['id']]['replies']))
        self.assertEqual([{'type': 'rec', 'x': 7}], self.rec_b.jobs)
        reply = self.a.sent_jobs[res['id']]['replies'][0]
        self.assertEqual('!00000002', reply['src'])
        self.assertEqual(7, reply['r']['value'])
        self.assertEqual([], self.rec_a.jobs)   # Ergebnis wird nicht als rec-Job ausgefuehrt

    def test_long_job_fragmented(self):
        pad = os.urandom(400).hex()            # 800 Zeichen, kaum komprimierbar
        res = self.a.send_job(dict(type='rec', x=1, pad=pad), dest=2)
        self.assertTrue(res['success'], res)
        self.assertGreater(res['fragments'], 2)
        self.assertTrue(wait_for(lambda: self.a.sent_jobs[res['id']]['replies']))
        self.assertEqual(pad, self.rec_b.jobs[0]['pad'])
        self.assertEqual(pad, self.a.sent_jobs[res['id']]['replies'][0]['r']['echo'])

    def test_too_large(self):
        self.a.max_fragments = 2
        res = self.a.send_job(dict(type='rec', pad=os.urandom(600).hex()), dest=2)
        self.assertFalse(res['success'])
        self.assertEqual([], self.ia.sent)

    def test_wrong_key_not_executed(self):
        key_c = os.path.join(self.tmp, 'c.key')
        make_key(key_c)
        c, ic, rec_c = self.node(3, key_c)
        ic.peer, self.ib.peer = self.ib, ic
        c.send_job(dict(type='rec', x=1), dest=2)
        time.sleep(0.3)
        self.assertEqual([], self.rec_b.jobs)
        self.assertEqual([], self.ib.sent)     # keine Antwort an Unbekannte

    def test_replay_ignored(self):
        self.ia.drop = True
        self.a.send_job(dict(type='rec', x=1), dest=2)
        self.assertTrue(wait_for(lambda: self.ia.sent))
        packet = {'from': 1, 'fromId': '!00000001', 'decoded': {'portnum': m.JOB_PORT, 'payload': self.ia.sent[0]}}
        self.b.onReceive(packet, self.ib)
        self.assertTrue(wait_for(lambda: self.rec_b.jobs))
        self.b.onReceive(packet, self.ib)
        time.sleep(0.3)
        self.assertEqual(1, len(self.rec_b.jobs))

    def test_too_old_rejected(self):
        self.b.max_age = 0.5
        self.ia.drop = True
        self.a.send_job(dict(type='rec', x=1), dest=2)
        self.assertTrue(wait_for(lambda: self.ia.sent))
        time.sleep(1.6)                          # Fernet-Zeitstempel hat Sekundenaufloesung
        self.b.onReceive({'from': 1, 'fromId': '!00000001',
                          'decoded': {'portnum': m.JOB_PORT, 'payload': self.ia.sent[0]}}, self.ib)
        time.sleep(0.3)
        self.assertEqual([], self.rec_b.jobs)

    def test_invalid_json_not_executed(self):
        token = self.a.crypt('encrypt_file', m.deflate(b'kein json'))
        import base64
        frags = m.split_fragments(base64.urlsafe_b64decode(token), 5)
        for f in frags:
            self.b.onReceive({'from': 1, 'fromId': '!00000001', 'decoded': {'portnum': m.JOB_PORT, 'payload': f}},
                             self.ib)
        time.sleep(0.3)
        self.assertEqual([], self.rec_b.jobs)
        self.assertEqual([], self.ib.sent)

    def test_pages_render(self):
        from nwebclient import base
        res = self.a.send_job(dict(type='rec', x=3), dest=2)
        self.assertTrue(wait_for(lambda: self.a.sent_jobs[res['id']]['replies']))
        p = base.Page()
        self.a.part_jobs(p)
        self.assertIn('rec', str(p))
        p = base.Page()
        self.b.part_jobs(p)
        self.assertIn('Empfangene Jobs', str(p))


def make_photo(w=1600, h=1200, mode='RGB', fmt='PNG') -> bytes:
    """ Testbild mit Verlauf und Rauschen, damit die JPEG nicht trivial klein wird """
    import io
    from PIL import Image
    im = Image.linear_gradient('L').resize((w, h)).convert('RGB')
    noise = Image.effect_noise((w, h), 40).convert('RGB')
    im = Image.blend(im, noise, 0.3).convert(mode)
    out = io.BytesIO()
    im.save(out, fmt)
    return out.getvalue()


class MeshImageTestCase(unittest.TestCase):

    def setUp(self):
        self.a = m.MeshtasticBot(fragment_delay=0)
        self.b = m.MeshtasticBot(fragment_delay=0)
        self.ia, self.ib = FakeIface(1), FakeIface(2)
        self.ia.bot, self.ib.bot = self.a, self.b
        self.ia.peer, self.ib.peer = self.ib, self.ia
        self.a.iface, self.b.iface = self.ia, self.ib

    def test_prepare_image(self):
        import io
        from PIL import Image
        jpeg, size, q = m.prepare_image(make_photo(), quality=30)
        im = Image.open(io.BytesIO(jpeg))
        self.assertEqual((600, 450), im.size)
        self.assertEqual('JPEG', im.format)
        self.assertTrue(im.info.get('progressive') or im.info.get('progression'))
        self.assertNotIn('exif', im.info)

    def test_prepare_image_rgba_and_quality_reduction(self):
        big, _, _ = m.prepare_image(make_photo(mode='RGBA'), quality=90)
        small, _, q = m.prepare_image(make_photo(mode='RGBA'), quality=90, max_bytes=len(big) // 2)
        self.assertLess(q, 90)
        self.assertLess(len(small), len(big))

    def test_transfer(self):
        res = self.a.execute(dict(type='mb', op='send_image', dest=2,
                                  data='data:image/png;base64,' + base64.b64encode(make_photo()).decode()))
        self.assertTrue(res['success'], res)
        self.assertEqual(600, res['width'])
        self.assertGreater(res['fragments'], 1)
        entry = lambda: next(iter(self.b.received_images.values()), None)
        self.assertTrue(wait_for(lambda: entry() is not None and entry()['done']))
        self.assertEqual(self.a.sent_images[res['id']]['jpeg'], self.b.image_data(entry()))
        self.assertEqual(res['fragments'], self.a.sent_images[res['id']]['sent'])

    def test_partial_preview(self):
        sent = []
        orig = self.ia.sendData

        def half(data, destinationId='^all', portNum=None, **kw):
            sent.append(data)
            if len(sent) <= 4:
                orig(data, destinationId, portNum, **kw)
        self.ia.sendData = half
        res = self.a.send_image(make_photo(), dest=2)
        self.assertTrue(wait_for(lambda: len(sent) == res['fragments']))
        e = next(iter(self.b.received_images.values()))
        self.assertFalse(e['done'])
        self.assertEqual(4, self.b.contiguous_parts(e))
        self.assertIsNotNone(m.preview_jpeg(self.b.image_data(e)))

    def lossy(self, drop):
        """ Bildpakete von a nach b verwerfen, drop(index, versuch) -> True = verlieren """
        orig, tries = self.ia.sendData, {}

        def send(data, destinationId='^all', portNum=None, **kw):
            body = data[len(m.IMAGE_MAGIC):]
            if data.startswith(m.IMAGE_MAGIC) and body[:1] == bytes([m.IMAGE_VERSION]):
                idx = m.IMAGE_HEADER.unpack(body[:m.IMAGE_HEADER.size])[2]
                tries[idx] = tries.get(idx, 0) + 1
                if drop(idx, tries[idx]):
                    return
            orig(data, destinationId, portNum, **kw)
        self.ia.sendData = send

    def image_entry(self):
        return next(iter(self.b.received_images.values()), None)

    def test_resend_recovers(self):
        self.b.image_resend_timeout = 0.3
        self.lossy(lambda idx, n: idx in (1, 2, 3, 7) and n == 1)   # nur beim ersten Versuch verloren
        res = self.a.send_image(make_photo(), dest=2)
        self.assertTrue(wait_for(lambda: self.image_entry() is not None and self.image_entry()['done']))
        e = self.image_entry()
        self.assertEqual(1, e['rounds'])
        self.assertEqual(4, e['requested'])
        self.assertEqual(4, self.a.sent_images[res['id']]['resent'])
        self.assertEqual(self.a.sent_images[res['id']]['jpeg'], self.b.image_data(e))

    def test_small_loss_accepted(self):
        self.b.image_resend_timeout = 0.3
        res = self.a.send_image(make_photo(), dest=2)
        last = res['fragments'] - 1             # erstes Bild ohne Verlust, nur um die Paketzahl zu kennen
        self.assertTrue(wait_for(lambda: self.image_entry() is not None and self.image_entry()['done']))
        self.b.received_images.clear()
        self.lossy(lambda idx, n: idx == last)  # nur das letzte Paket fehlt: > 90 % nutzbar
        res = self.a.send_image(make_photo(), dest=2)
        self.assertTrue(wait_for(lambda: self.image_entry() is not None and self.image_entry()['state'] == 'accepted'))
        self.assertEqual(0, self.image_entry()['rounds'])
        self.assertEqual(0, self.a.sent_images[res['id']]['resent'])

    def test_gives_up(self):
        self.b.image_resend_timeout = 0.2
        self.a.image_max_rounds = self.b.image_max_rounds = 2
        self.lossy(lambda idx, n: idx == 1)     # Paket 1 kommt nie an
        res = self.a.send_image(make_photo(), dest=2)
        self.assertTrue(wait_for(lambda: self.image_entry() is not None
                                 and self.image_entry()['state'] == 'given_up'))
        self.assertEqual(2, self.image_entry()['rounds'])
        self.assertEqual(2, self.a.sent_images[res['id']]['resent'])
        self.assertEqual(1, self.b.contiguous_parts(self.image_entry()))

    def test_port_name_and_foreign_payload(self):
        # die Python-Bibliothek liefert bekannte Ports als Namen
        self.assertEqual(256, m.MeshtasticBot.portnum({'decoded': {'portnum': 'PRIVATE_APP'}}))
        res = self.a.send_image(make_photo(), dest=2)
        self.assertTrue(wait_for(lambda: self.a.sent_images[res['id']]['sent'] == res['fragments']))
        # Paket einer anderen privaten App auf Port 256 wird ignoriert
        self.b.received_images.clear()
        self.b.onReceive({'from': 1, 'fromId': '!00000001',
                          'decoded': {'portnum': 'PRIVATE_APP', 'payload': b'\x01\x00\x05\x00\x00\x00\x03xyz'}},
                         self.ib)
        self.assertEqual(0, len(self.b.received_images))
        self.assertTrue(all(len(f) <= 227 for f in self.ia.sent))

    def test_to_ranges(self):
        self.assertEqual([(2, 3), (9, 1)], m.to_ranges([2, 3, 4, 9]))
        self.assertEqual([], m.to_ranges([]))

    def test_invalid_image(self):
        res = self.a.execute(dict(type='mb', op='send_image', data=base64.b64encode(b'kein bild').decode()))
        self.assertFalse(res['success'])

    def test_page_renders(self):
        from nwebclient import base
        res = self.a.send_image(make_photo(), dest=2)
        self.assertTrue(wait_for(lambda: self.a.sent_images[res['id']]['sent'] == res['fragments']))
        for bot in (self.a, self.b):
            p = base.Page()
            bot.part_images(p)
            self.assertIn('data:image/jpeg;base64,', str(p))
            self.assertIn('image_data', str(p))


if __name__ == '__main__':
    unittest.main()
