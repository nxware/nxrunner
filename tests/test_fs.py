"""
Unittests fuer nxrunner.fs (Browse, BrowseRemote)

    python -m unittest tests.test_fs
"""
import os
import sqlite3
import tempfile
import threading
import unittest

from nwebclient import base as b

from nxrunner import fs
from nxrunner import jobstore as js
from tests.test_jobstore import create_root, NoPollMixin


class FakeNWeb:
    """ nweb-Persistence mit dml/select auf SQLite im Speicher """

    param = '?'

    def __init__(self):
        self.db = sqlite3.connect(':memory:', check_same_thread=False)
        self.lock = threading.RLock()

    def dml(self, sql, vals=[], commit=True):
        with self.lock:
            self.db.execute(sql, vals)
            self.db.commit()

    def select(self, sql, params=[]):
        with self.lock:
            return self.db.execute(sql, params).fetchall()


class TestBrowseRemote(NoPollMixin, fs.BrowseRemote):
    pass


def make_tree():
    tmp = tempfile.mkdtemp()
    os.makedirs(os.path.join(tmp, 'sub'))
    with open(os.path.join(tmp, 'a.txt'), 'w', encoding='utf-8') as f:
        f.write('hallo')
    with open(os.path.join(tmp, 'sub', 'b.bin'), 'wb') as f:
        f.write(b'\0\1\2')
    return tmp


class BrowseTestCase(unittest.TestCase):

    def setUp(self):
        self.browse = fs.Browse(path=make_tree())

    def test_listdir(self):
        res = self.browse.listdir('')
        self.assertEqual([(e['name'], e['dir']) for e in res['entries']], [('sub', True), ('a.txt', False)])
        self.assertEqual(self.browse.listdir('sub')['entries'][0]['path'], 'sub/b.bin')

    def test_outside_path(self):
        self.assertEqual(self.browse.listdir('../..')['dir'], '')
        self.assertFalse(self.browse.fileinfo('../a.txt')['success'])

    def test_fileinfo(self):
        info = self.browse.fileinfo('a.txt')['info']
        self.assertEqual((info['preview'], info['size'], info['binary']), ('hallo', 5, False))
        self.assertTrue(self.browse.fileinfo('sub/b.bin')['info']['binary'])

    def test_pages(self):
        p = b.Page(owner=self.browse)
        self.browse.part_index(p, {})
        self.assertIn('a.txt', str(p))
        p = b.Page(owner=self.browse)
        self.browse.part_file(p, {'file': 'a.txt'})
        self.assertIn('hallo', str(p))


class BrowseRemoteTestCase(unittest.TestCase):

    def setUp(self):
        self.browse = fs.Browse(path=make_tree())
        self.store = js.DirectJobStore(type='store')
        self.remote = TestBrowseRemote(type='rbrowse', target='local', store='store')
        self.root = create_root(self.store, self.browse, self.remote)
        self.nw = FakeNWeb()
        self.root.nweb = self.nw

    def rows(self):
        return self.nw.select("SELECT guid, op, arg, status FROM browse_remote_job ORDER BY sent")

    def poll_all(self, runner=None):
        runner = runner or self.remote
        for guid in list(runner.remote_pending.keys()):
            runner.remote_poll(guid)

    def test_request_saved_and_answered(self):
        e = self.remote.request('listdir', '/sub/')
        self.assertEqual((e['arg'], e['status']), ('sub', 'pending'))
        self.assertEqual(self.rows(), [(e['guid'], 'listdir', 'sub', 'pending')])
        self.poll_all()
        self.assertEqual(self.rows()[0][3], 'done')
        self.assertEqual(self.remote.last_result('listdir', 'sub')['entries'][0]['name'], 'b.bin')

    def test_restart_polls_stored_guid(self):
        guid = self.remote.request('fileinfo', 'a.txt')['guid']
        other = TestBrowseRemote(type='rbrowse', target='local', store='store')
        self.root.jobexecutor.add_runner(other)  # ersetzt den alten Runner, wie nach einem Neustart
        with other.remote_lock:
            other.ensure_loaded()
        self.assertIn(guid, other.remote_pending)
        self.poll_all(other)
        self.assertEqual(other.last_result('fileinfo', 'a.txt')['info']['preview'], 'hallo')
        self.assertEqual(self.rows()[0][3], 'done')

    def test_restart_shows_stored_result(self):
        self.remote.request('listdir', '')
        self.poll_all()
        other = TestBrowseRemote(type='rbrowse', target='local', store='store')
        self.root.jobexecutor.add_runner(other)
        p = b.Page(owner=other)
        other.part_index(p, {})
        self.assertIn('a.txt', str(p))
        self.assertEqual(other.remote_pending, {})

    def test_refresh_replaces_old_result(self):
        first = self.remote.request('listdir', '')['guid']
        self.poll_all()
        res = self.remote.refresh('listdir', '')
        self.assertTrue(res['success'], res)
        self.assertEqual({r[0] for r in self.rows()}, {first, res['guid']})
        self.poll_all()
        self.assertEqual([r[0] for r in self.rows()], [res['guid']])

    def test_timeout(self):
        self.store.set_delay(100)
        self.remote.timeout = -1
        guid = self.remote.request('listdir', '')['guid']
        self.poll_all()
        self.assertEqual(self.rows(), [(guid, 'listdir', '', 'timeout')])

    def test_error(self):
        self.remote.request('fileinfo', 'fehlt.txt')
        self.poll_all()
        self.assertEqual(self.rows()[0][3], 'error')
        p = b.Page(owner=self.remote)
        self.remote.part_file(p, {'file': 'fehlt.txt'})
        self.assertIn('Aktualisieren', str(p))

    def test_without_persistence(self):
        self.root.nweb = None
        self.remote.request('listdir', '')
        self.poll_all()
        self.assertIsNotNone(self.remote.last_result('listdir', ''))

    def test_pending_page_reloads(self):
        self.store.set_delay(100)
        p = b.Page(owner=self.remote)
        self.remote.part_index(p, {})
        self.assertIn('noch nicht beantwortet', str(p))
        self.assertIn('window.location.reload', str(p))


if __name__ == '__main__':
    unittest.main()
