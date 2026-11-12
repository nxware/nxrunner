"""
Unittests fuer nxrunner.jobstore

    python -m unittest tests.test_jobstore
"""
import json
import time
import unittest

from nwebclient import runner as r

from nxrunner import jobstore as js


# --- Hilfsklassen ---------------------------------------------------------------------------------

class Recorder(r.BaseJobExecutor):
    """ Merkt sich alle Jobs und liefert ein festes Ergebnis """

    def __init__(self, type='rec', result=None):
        super().__init__(type)
        self.jobs = []
        self.result = {'success': True, 'value': 42} if result is None else result

    def execute(self, data):
        self.jobs.append(dict(data))
        return self.result if not isinstance(self.result, dict) else dict(self.result)


class Failing(r.BaseJobExecutor):
    def __init__(self, type='boom'):
        super().__init__(type)

    def execute(self, data):
        raise RuntimeError('kaputt')


class FakeCrypt(r.BaseJobExecutor):
    """ Ersatz fuer den CryptRunner: 'verschluesselt' durch Umdrehen mit Praefix """

    PREFIX = 'enc:'

    def __init__(self):
        super().__init__('crypt')

    def execute(self, data):
        if data.get('op') == 'encrypt':
            return self.success(value=self.PREFIX + str(data['value'])[::-1])
        if data.get('op') == 'decrypt':
            c = str(data['c'])
            if not c.startswith(self.PREFIX):
                return self.fail('invalid token')
            return self.success(value=c[len(self.PREFIX):][::-1])
        return self.fail('unknown')


class FakeDoc:
    def __init__(self, id, title, content, group, kind=None, guid=None):
        self._id = id
        self.title = title
        self._content = content
        self.group = str(group)
        self.kind = kind
        self._guid = guid if guid is not None else f'doc-{id}'

    def id(self):
        return self._id

    def content(self):
        return self._content

    def guid(self):
        return self._guid


class FakeNWeb:
    """ Minimaler NWebClient: docs, createDoc, deleteDoc im Speicher """

    def __init__(self):
        self.documents = {}
        self.next_id = 1

    def docs(self, query):
        group = query.split('=', 1)[1]
        return [doc for doc in self.documents.values() if doc.group == group]

    def createDoc(self, title, content, group, kind=None, guid=None):
        doc = FakeDoc(self.next_id, title, content, group, kind, guid)
        self.documents[doc.id()] = doc
        self.next_id += 1
        return doc

    def deleteDoc(self, id):
        self.documents.pop(id, None)

    def group_docs(self, group):
        return [json.loads(doc.content()) for doc in self.docs('group_id=' + str(group))]


class FakeSettings:
    """ nweb-Persistence mit setting/set_setting im Speicher """

    def __init__(self):
        self.values = {}

    def setting(self, name, default=None):
        return self.values.get(name, default)

    def set_setting(self, name, value):
        self.values[name] = value


def create_root(*runners):
    """ JobRunner mit LazyDispatcher, damit getRoot().jobexecutor funktioniert """
    dispatcher = r.LazyDispatcher()
    for runner in runners:
        dispatcher.add_runner(runner)
    root = r.JobRunner(dispatcher, {})
    for runner in runners:
        dispatcher.get_runner(runner.type)  # erzeugen und einhaengen
    return root


class NoPollMixin:
    """ Abfrage-Thread unterdruecken, remote_poll wird im Test direkt aufgerufen """

    def start_remote_poller(self):
        self.poller_started = True


class TestRemote(NoPollMixin, r.BaseJobExecutor, js.RemoteJobMixin):

    def __init__(self, **kwargs):
        super().__init__('remote_test')
        self.init_remote(**kwargs)
        self.results = []
        self.changes = 0

    def on_remote_result(self, meta, result):
        self.results.append((meta, result))

    def remote_changed(self):
        self.changes += 1


class TestRemoteRunner(NoPollMixin, js.RemoteRunner):
    pass


# --- JobStoreMixin --------------------------------------------------------------------------------

class ParseJobdefTestCase(unittest.TestCase):

    def setUp(self):
        self.store = js.DirectJobStore()

    def test_dict(self):
        job, error = self.store.parse_jobdef({'jobdef': {'type': 'x'}})
        self.assertEqual(job, {'type': 'x'})
        self.assertIsNone(error)

    def test_json_string(self):
        job, error = self.store.parse_jobdef({'jobdef': '{"type": "x", "a": 1}'})
        self.assertEqual(job, {'type': 'x', 'a': 1})
        self.assertIsNone(error)

    def test_empty(self):
        job, error = self.store.parse_jobdef({'jobdef': '  '})
        self.assertIsNone(job)
        self.assertIn('empty', error)

    def test_invalid_json(self):
        job, error = self.store.parse_jobdef({'jobdef': '{kein json'})
        self.assertIsNone(job)
        self.assertIn('JSON', error)

    def test_without_type(self):
        for jobdef in ({'a': 1}, '[1, 2]', None):
            job, error = self.store.parse_jobdef({'jobdef': jobdef})
            self.assertIsNone(job, jobdef)
            self.assertIn('type', error)


class IsJobStoreTestCase(unittest.TestCase):

    def test_mixin(self):
        self.assertTrue(js.JobStoreMixin.is_job_store(js.DirectJobStore()))

    def test_duck_typing(self):
        class Duck:
            def execute_cenqueue(self, data): pass
            def execute_cresult(self, data): pass
        self.assertTrue(js.JobStoreMixin.is_job_store(Duck()))

    def test_other_runner(self):
        self.assertFalse(js.JobStoreMixin.is_job_store(Recorder()))


class AbstractStoreTestCase(unittest.TestCase):

    class Store(r.BaseJobExecutor, js.JobStoreMixin):
        def __init__(self):
            super().__init__('abstract_store')

    def test_not_implemented(self):
        store = self.Store()
        res = store.execute_cenqueue({'jobdef': {'type': 'x'}})
        self.assertFalse(res['success'])
        self.assertIn('NotImplementedError', res['message'])
        res = store.execute_cresult({'guid': 'g'})
        self.assertFalse(res['success'])
        self.assertIn('NotImplementedError', res['message'])


class CryptTestCase(unittest.TestCase):

    def test_has_crypt(self):
        store = js.DirectJobStore()
        create_root(store)
        self.assertFalse(store.has_crypt())
        store = js.DirectJobStore()
        create_root(store, FakeCrypt())
        self.assertTrue(store.has_crypt())

    def test_roundtrip(self):
        store = js.DirectJobStore()
        create_root(store, FakeCrypt())
        c = store.encrypt('geheim')
        self.assertNotEqual(c, 'geheim')
        self.assertEqual(store.decrypt(c), 'geheim')

    def test_decrypt_fail(self):
        store = js.DirectJobStore()
        create_root(store, FakeCrypt())
        with self.assertRaises(Exception) as ctx:
            store.decrypt('kaputt')
        self.assertIn('invalid token', str(ctx.exception))


# --- DirectJobStore -------------------------------------------------------------------------------

class DirectJobStoreTestCase(unittest.TestCase):

    def setUp(self):
        self.rec = Recorder()
        self.store = js.DirectJobStore()
        self.root = create_root(self.store, self.rec, Failing())

    def enqueue(self, job, **kwargs):
        return self.root.jobexecutor.execute(dict(type=self.store.type, op='cenqueue', jobdef=json.dumps(job), **kwargs))

    def result(self, guid, **kwargs):
        return self.root.jobexecutor.execute(dict(type=self.store.type, op='cresult', guid=guid, **kwargs))

    def test_enqueue_and_result(self):
        res = self.enqueue({'type': 'rec', 'a': 1}, guid='g1')
        self.assertTrue(res['success'], res)
        self.assertEqual(res['result'], 'g1')
        self.assertEqual(self.rec.jobs, [{'type': 'rec', 'a': 1}])
        res = self.result('g1')
        self.assertTrue(res['success'])
        self.assertFalse(res['pending'])
        self.assertEqual(res['result']['value'], 42)

    def test_generated_guid(self):
        res = self.enqueue({'type': 'rec'})
        self.assertTrue(res['success'])
        self.assertTrue(res['result'])
        self.assertFalse(self.result(res['result'])['pending'])

    def test_delete(self):
        self.enqueue({'type': 'rec'}, guid='g1')
        self.assertFalse(self.result('g1', delete=0)['pending'])
        self.assertFalse(self.result('g1', delete='false')['pending'])
        self.assertFalse(self.result('g1')['pending'])
        self.assertTrue(self.result('g1')['pending'])

    def test_other_results_untouched(self):
        self.enqueue({'type': 'rec'}, guid='g1')
        self.enqueue({'type': 'rec'}, guid='g2')
        self.result('g1')
        self.assertFalse(self.result('g2')['pending'])

    def test_unknown_guid_pending(self):
        res = self.result('unbekannt')
        self.assertTrue(res['success'])
        self.assertTrue(res['pending'])

    def test_missing_guid(self):
        res = self.result('  ')
        self.assertFalse(res['success'])

    def test_invalid_jobdef(self):
        res = self.enqueue_raw('{nope')
        self.assertFalse(res['success'])
        self.assertEqual(self.rec.jobs, [])

    def enqueue_raw(self, jobdef):
        return self.store.execute_cenqueue({'jobdef': jobdef})

    def test_failing_job(self):
        self.enqueue({'type': 'boom'}, guid='g1')
        res = self.result('g1')
        self.assertFalse(res['pending'])
        self.assertFalse(res['result']['success'])
        self.assertIn('kaputt', res['result']['message'])

    def test_non_dict_result(self):
        self.rec.result = 'text'
        self.enqueue({'type': 'rec'}, guid='g1')
        self.assertEqual(self.result('g1')['result'], {'result': 'text'})

    def test_delay(self):
        self.store.set_delay('60')
        self.assertEqual(self.store.delay, 60.0)
        self.enqueue({'type': 'rec'}, guid='g1')
        self.assertTrue(self.result('g1')['pending'])
        ts, value = self.store.results['g1']
        self.store.results['g1'] = (time.time() - 1, value)
        self.assertFalse(self.result('g1')['pending'])

    def test_set_delay_none(self):
        for value in (None, '', 'None'):
            self.store.set_delay(5)
            self.store.set_delay(value)
            self.assertIsNone(self.store.delay)

    def test_signatures(self):
        sigs = self.store.execute_get_signatures({})['signatures']
        ops = {sig['job'].get('op') for sig in sigs}
        self.assertTrue({'cenqueue', 'cresult'} <= ops, ops)


# --- NWebJobRunner --------------------------------------------------------------------------------

class NWebJobRunnerTestCase(unittest.TestCase):

    GROUP = 'jobs'
    UPLOAD = 'results'

    def setUp(self):
        self.nweb = FakeNWeb()
        self.rec = Recorder()
        self.runner = js.NWebJobRunner(interval=None, nwebclient=self.nweb, group=self.GROUP, limit=None)
        self.runner.group_upload = self.UPLOAD
        self.runner.instance_name = 'me'
        self.crypt = FakeCrypt()
        self.root = create_root(self.runner, self.rec, self.crypt)

    def add_job(self, job):
        return self.nweb.createDoc('job', json.dumps(job), self.GROUP, kind='json')

    def test_can_process_target(self):
        self.assertTrue(self.runner.canProcess({'type': 'rec'}))
        self.assertTrue(self.runner.canProcess({'type': 'rec', 'target': 'me'}))
        self.assertFalse(self.runner.canProcess({'type': 'rec', 'target': 'other'}))

    def test_can_process_types(self):
        self.runner.reject_types = ['bad']
        self.assertFalse(self.runner.canProcess({'type': 'bad'}))
        self.runner.default_can_process = False
        self.assertFalse(self.runner.canProcess({'type': 'rec'}))
        self.runner.add_type('rec')
        self.assertTrue(self.runner.canProcess({'type': 'rec'}))

    def test_tick_executes_in_order(self):
        self.add_job({'type': 'rec', 'n': 1})
        self.add_job({'type': 'rec', 'n': 2, 'target': 'me'})
        res = self.runner.tick()
        self.assertEqual(res['count'], 2)
        self.assertEqual([j['n'] for j in self.rec.jobs], [1, 2])
        self.assertNotIn('target', self.rec.jobs[1])
        self.assertEqual(self.nweb.docs('group_id=' + self.GROUP), [], "Jobs werden geloescht")
        self.assertEqual(self.runner.counter, 2)

    def test_tick_skips_other_target(self):
        self.add_job({'type': 'rec', 'target': 'other'})
        self.assertEqual(self.runner.tick()['count'], 0)
        self.assertEqual(len(self.nweb.docs('group_id=' + self.GROUP)), 1, "Job bleibt liegen")

    def test_tick_limit(self):
        self.runner.limit = 1
        self.add_job({'type': 'rec', 'n': 1})
        self.add_job({'type': 'rec', 'n': 2})
        self.assertEqual(self.runner.tick()['count'], 1)
        self.assertEqual([j['n'] for j in self.rec.jobs], [1])

    def test_keep(self):
        self.add_job({'type': 'rec', 'keep': True})
        self.runner.tick()
        self.assertEqual(len(self.nweb.docs('group_id=' + self.GROUP)), 1)

    def test_plain_result_upload(self):
        self.add_job({'type': 'rec', 'guid': 'g1'})
        self.runner.tick()
        docs = self.nweb.docs('group_id=' + self.UPLOAD)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0].guid(), 'g1')
        self.assertEqual(json.loads(docs[0].content())['value'], 42)

    def test_enqueue_plain_and_result(self):
        res = self.runner.execute({'op': 'enqueue', 'jobdef': '{"type": "rec", "a": 1}', 'guid': 'g1', 'target': 'me'})
        self.assertTrue(res['success'], res)
        stored = self.nweb.group_docs(self.GROUP)[0]
        self.assertEqual(stored['type'], 'rec')
        self.assertEqual(stored['guid'], 'g1')
        self.assertEqual(stored['target'], 'me')
        self.assertIn('ts', stored)
        self.assertTrue(self.runner.execute({'op': 'result', 'guid': 'g1'})['pending'])
        self.runner.tick()
        res = self.runner.execute({'op': 'result', 'guid': 'G1'})
        self.assertFalse(res['pending'])
        self.assertEqual(res['result']['value'], 42)
        self.assertNotIn('guid', res['result'])
        self.assertTrue(self.runner.execute({'op': 'result', 'guid': 'g1'})['pending'], "nach delete weg")

    def test_crypt_roundtrip(self):
        res = self.runner.execute({'op': 'cenqueue', 'jobdef': {'type': 'rec', 'secret': 'xyz'}, 'guid': 'g1'})
        self.assertTrue(res['success'], res)
        stored = self.nweb.group_docs(self.GROUP)[0]
        self.assertEqual(stored['type'], 'crypt')
        self.assertNotIn('xyz', json.dumps(stored), "Job liegt verschluesselt")
        self.assertEqual(stored['guid'], 'g1')
        # verschluesselten Job (c ohne op) ausfuehren: der FakeCrypt kennt das nicht, daher festes Ergebnis
        self.crypt.execute = lambda data: (dict(success=True, value=7) if 'op' not in data
                                           else FakeCrypt.execute(self.crypt, data))
        self.runner.tick()
        uploaded = self.nweb.group_docs(self.UPLOAD)[0]
        self.assertIn('c', uploaded)
        self.assertEqual(uploaded['guid'], 'g1')
        res = self.runner.execute({'op': 'cresult', 'guid': 'g1', 'delete': 0})
        self.assertFalse(res['pending'])
        self.assertEqual(res['result'], {'success': True, 'value': 7})

    def test_cenqueue_without_crypt(self):
        runner = js.NWebJobRunner(interval=None, nwebclient=FakeNWeb(), group=self.GROUP)
        create_root(runner)
        res = runner.execute({'op': 'cenqueue', 'jobdef': {'type': 'rec'}})
        self.assertFalse(res['success'])
        self.assertIn('crypt', res['message'])

    def test_enqueue_without_group(self):
        self.runner.group = None
        res = self.runner.execute({'op': 'enqueue', 'jobdef': {'type': 'rec'}})
        self.assertFalse(res['success'])
        self.assertIn(js.NWebJobRunner.CFG_FETCH_ID, res['message'])

    def test_result_without_upload_group(self):
        self.runner.group_upload = None
        res = self.runner.execute({'op': 'cresult', 'guid': 'g1'})
        self.assertFalse(res['success'])
        self.assertIn(js.NWebJobRunner.CFG_RESULT_ID, res['message'])

    def test_failing_job_not_deleted(self):
        root = create_root(Failing())
        runner = js.NWebJobRunner(interval=None, nwebclient=self.nweb, group=self.GROUP)
        root.jobexecutor.add_runner(runner)
        root.jobexecutor.get_runner(runner.type)
        self.add_job({'type': 'boom'})
        self.assertEqual(runner.tick()['count'], 0)
        self.assertEqual(len(self.nweb.docs('group_id=' + self.GROUP)), 1)


# --- RemoteJobMixin -------------------------------------------------------------------------------

class RemoteJobMixinTestCase(unittest.TestCase):

    def setUp(self):
        self.rec = Recorder()
        self.store = js.DirectJobStore(type='store')
        self.remote = TestRemote(target='local', store='store', remote_type='rec', timeout=100)
        self.root = create_root(self.store, self.rec, self.remote)

    def test_send_and_poll(self):
        guid = self.remote.remote_send({'op': 'x'}, kind='test')
        self.assertTrue(self.remote.poller_started)
        self.assertEqual(self.rec.jobs, [{'type': 'rec', 'op': 'x'}])
        meta = self.remote.remote_pending[guid]
        self.assertEqual(meta['kind'], 'test')
        self.assertEqual(meta['seq'], 1)
        self.assertEqual(meta['label'], 'x')
        self.remote.remote_poll(guid)
        self.assertEqual(self.remote.remote_pending, {})
        self.assertEqual(len(self.remote.results), 1)
        self.assertEqual(self.remote.results[0][1]['value'], 42)
        self.assertIsNotNone(self.remote.remote_last_update)
        self.assertEqual(self.remote.remote_error, '')

    def test_send_without_target(self):
        self.remote.target = ''
        with self.assertRaises(RuntimeError):
            self.remote.remote_send({'op': 'x'})

    def test_check_store(self):
        self.assertEqual(self.remote.check_store(), '')
        self.remote.store = 'fehlt'
        self.assertIn('nicht geladen', self.remote.check_store())
        self.remote.store = 'rec'
        self.assertIn('kein Job-Speicher', self.remote.check_store())

    def test_send_invalid_store(self):
        self.remote.store = 'rec'
        with self.assertRaises(RuntimeError):
            self.remote.remote_send({'op': 'x'})
        self.assertFalse(self.remote.store_ok)

    def test_poll_pending_and_timeout(self):
        self.store.delay = 1000
        guid = self.remote.remote_send({'op': 'x'})
        self.remote.remote_poll(guid)
        self.assertIn(guid, self.remote.remote_pending)
        self.remote.remote_pending[guid]['ts'] -= 1000
        self.remote.remote_poll(guid)
        self.assertNotIn(guid, self.remote.remote_pending)
        self.assertIn('Keine Antwort', self.remote.remote_error)
        self.assertEqual(self.remote.results, [])

    def test_poll_remote_error(self):
        self.rec.result = {'success': False, 'message': 'remote kaputt'}
        guid = self.remote.remote_send({'op': 'x'})
        self.remote.remote_poll(guid)
        self.assertEqual(self.remote.remote_error, 'remote kaputt')
        self.assertEqual(len(self.remote.results), 1)

    def test_poll_unknown_guid(self):
        changes = self.remote.changes
        self.remote.remote_poll('unbekannt')
        self.assertEqual(self.remote.changes, changes)

    def test_status(self):
        self.store.delay = 1000
        self.assertEqual(self.remote.remote_status(), 'local')
        self.remote.remote_send({'op': 'eins'})
        self.remote.remote_send({'op': 'zwei'})
        status = self.remote.remote_status()
        self.assertIn('2 Antwort', status)
        self.assertIn('eins, zwei', status)
        self.assertTrue(self.remote.remote_status(max_len=5).endswith('...'))
        self.remote.set_remote_error('Fehler X')
        self.assertTrue(self.remote.remote_status().endswith('Fehler X'))

    def test_status_last_update(self):
        guid = self.remote.remote_send({'op': 'x'})
        self.remote.remote_poll(guid)
        self.assertIn('Stand', self.remote.remote_status())

    def test_poll_loop_ends_without_pending(self):
        self.remote.poll_interval = 0
        self.remote.remote_poller = object()
        self.remote.remote_poll_loop()
        self.assertIsNone(self.remote.remote_poller)


# --- RemoteRunner ---------------------------------------------------------------------------------

class RemoteRunnerTestCase(unittest.TestCase):

    def setUp(self):
        self.rec = Recorder()
        self.store = js.DirectJobStore(type='store')
        self.runner = TestRemoteRunner(target='local', store='store', max_results=3)
        self.root = create_root(self.store, self.rec, self.runner)

    def run_job(self, job):
        return self.runner.execute({'op': 'run', 'jobdef': json.dumps(job)})

    def poll_all(self):
        for guid in list(self.runner.remote_pending.keys()):
            self.runner.remote_poll(guid)

    def test_job_label(self):
        self.assertEqual(js.RemoteRunner.job_label({'type': 'rec'}), 'rec')
        self.assertEqual(js.RemoteRunner.job_label({'type': 'rec', 'op': 'x'}), 'rec x')
        self.assertEqual(self.runner.remote_job_label({'type': 'list_runners'}), 'Runner abrufen')
        self.assertEqual(self.runner.remote_job_label({'type': 'a', 'op': 'get_signatures'}), 'Signaturen a')

    def test_run_and_result(self):
        res = self.run_job({'type': 'rec', 'op': 'x'})
        self.assertTrue(res['success'], res)
        guid = res['guid']
        self.assertEqual(self.runner.execute({'op': 'result', 'guid': guid})['result']['status'], 'pending')
        self.poll_all()
        job = self.runner.execute({'op': 'result', 'guid': guid})['result']
        self.assertEqual(job['status'], 'ok')
        self.assertEqual(job['result']['value'], 42)
        self.assertIsNotNone(job['done'])

    def test_run_error_result(self):
        self.rec.result = {'success': False, 'message': 'nein'}
        guid = self.run_job({'type': 'rec'})['guid']
        self.poll_all()
        self.assertEqual(self.runner.jobs[guid]['status'], 'error')

    def test_run_invalid_jobdef(self):
        res = self.runner.execute({'op': 'run', 'jobdef': '{}'})
        self.assertFalse(res['success'])
        self.assertEqual(self.runner.jobs, {})

    def test_run_without_store(self):
        self.runner.store = 'fehlt'
        res = self.run_job({'type': 'rec'})
        self.assertFalse(res['success'])
        self.assertIn('fehlt', self.runner.remote_error)

    def test_timeout_marks_job(self):
        self.store.delay = 1000
        guid = self.run_job({'type': 'rec'})['guid']
        self.runner.remote_pending[guid]['ts'] -= self.runner.timeout + 1
        self.poll_all()
        self.assertEqual(self.runner.jobs[guid]['status'], 'timeout')

    def test_unknown_result(self):
        self.assertFalse(self.runner.execute({'op': 'result', 'guid': 'x'})['success'])

    def test_results_and_clear(self):
        self.store.delay = 1000
        pending = self.run_job({'type': 'rec'})['guid']
        self.store.delay = None
        done = self.run_job({'type': 'rec'})['guid']
        self.runner.remote_poll(done)
        self.assertEqual(len(self.runner.execute({'op': 'results'})['results']), 2)
        res = self.runner.execute({'op': 'clear_results'})
        self.assertIn('1 Ergebnis', res['message'])
        self.assertEqual(list(self.runner.jobs.keys()), [pending])
        res = self.runner.execute({'op': 'clear_results', 'guid': pending})
        self.assertIn('1 Ergebnis', res['message'])
        self.assertEqual(self.runner.jobs, {})

    def test_trim_jobs(self):
        guids = [self.run_job({'type': 'rec', 'n': i})['guid'] for i in range(3)]
        self.poll_all()
        self.run_job({'type': 'rec', 'n': 3})
        self.assertEqual(len(self.runner.jobs), 3)
        self.assertNotIn(guids[0], self.runner.jobs)

    def test_trim_keeps_pending(self):
        self.store.delay = 1000
        for i in range(4):
            self.run_job({'type': 'rec', 'n': i})
        self.assertEqual(len(self.runner.jobs), 4, "ausstehende Jobs werden nicht verworfen")

    def test_on_signatures(self):
        with self.runner.remote_lock:
            self.runner.on_signatures('a', {'success': True, 'signatures': [{'name': 'x'}]})
            self.runner.on_signatures('b', {'success': False, 'message': 'weg'})
        runners = self.runner.execute({'op': 'runners'})['runners']
        self.assertEqual(runners['a'], {'signatures': [{'name': 'x'}], 'error': ''})
        self.assertEqual(runners['b'], {'signatures': None, 'error': 'weg'})

    def test_on_runner_list(self):
        self.runner.extra_runners = ['extra']
        self.runner.request_signatures = lambda names: None
        with self.runner.remote_lock:
            self.runner.on_runner_list({'names': ['b', 'a']})
        self.assertEqual(sorted(self.runner.remote_runners.keys()), ['a', 'b', 'extra'])
        with self.runner.remote_lock:
            self.runner.on_runner_list({'message': 'nix'})
        self.assertIn('nix', self.runner.remote_error)

    def test_on_runner_list_empty(self):
        self.runner.request_signatures = lambda names: None
        with self.runner.remote_lock:
            self.runner.on_runner_list({'names': []})
        self.assertIn('meldet keine Runner', self.runner.remote_error)

    def test_index_while_refresh_pending(self):
        from nwebclient import base as b
        self.store.delay = 1000
        self.assertTrue(self.runner.execute({'op': 'refresh'})['success'])
        p = b.Page()
        self.runner.part_index(p, {})
        html = str(p)
        self.assertIn(f"Abfrage {self.runner.refresh['guid']}", html)
        self.assertIn('noch nicht ausgefuehrt', html)
        self.assertNotIn('Noch keine Runner bekannt', html)
        self.assertIn('window.location.reload', html)

    def test_refresh_remembers_guid(self):
        self.store.delay = 1000
        self.runner.execute({'op': 'refresh'})
        refresh = self.runner.refresh
        self.assertEqual(refresh['status'], 'pending')
        self.assertIn(refresh['guid'], self.runner.remote_pending)
        self.assertEqual(self.runner.remote_pending[refresh['guid']]['kind'], 'list')
        self.assertEqual(self.runner.execute({'op': 'runners'})['refresh']['guid'], refresh['guid'])

    def test_page_checks_store_immediately(self):
        """ Ergebnis liegt im Speicher: die Seite holt es per guid ab, ohne auf den Poll-Thread zu warten """
        from nwebclient import base as b
        self.runner.extra_runners = ['store']
        self.runner.request_signatures = lambda names: None
        self.runner.execute({'op': 'refresh'})
        guid = self.runner.refresh['guid']
        self.assertIn(guid, self.store.results)
        p = b.Page()
        self.runner.part_index(p, {})
        self.assertEqual(self.runner.refresh['status'], 'done')
        self.assertNotIn(guid, self.store.results, "Ergebnis abgeholt")
        self.assertIn('ausgefuehrt', str(p))
        self.assertIn('store (?)', str(p))
        self.assertNotIn('window.location.reload', str(p))

    def test_refresh_status_op(self):
        self.store.delay = 1000
        self.runner.execute({'op': 'refresh'})
        self.assertEqual(self.runner.execute({'op': 'refresh_status'})['refresh']['status'], 'pending')
        guid = self.runner.refresh['guid']
        self.store.results[guid] = (time.time() - 1, self.store.results[guid][1])
        self.runner.request_signatures = lambda names: None
        self.assertEqual(self.runner.execute({'op': 'refresh_status'})['refresh']['status'], 'done')

    def test_refresh_survives_restart(self):
        """ guid wird gespeichert, ein neuer Runner (Neustart) prueft die Abfrage weiter """
        settings = FakeSettings()
        self.root.nweb = settings
        self.store.delay = 1000
        self.runner.execute({'op': 'refresh'})
        guid = self.runner.refresh['guid']
        self.assertEqual(json.loads(settings.values['remote_runner.refresh.local'])['guid'], guid)
        restarted = TestRemoteRunner(target='local', store='store')
        restarted.request_signatures = lambda names: None
        create_root(self.store, self.rec, restarted).nweb = settings
        self.store.results[guid] = (time.time() - 1, self.store.results[guid][1])
        restarted.check_refresh()
        self.assertEqual(restarted.refresh['guid'], guid)
        self.assertEqual(restarted.refresh['status'], 'done')
        self.assertEqual(json.loads(settings.values['remote_runner.refresh.local'])['status'], 'done')

    def test_refresh_timeout(self):
        from nwebclient import base as b
        self.store.delay = 1000
        self.runner.execute({'op': 'refresh'})
        guid = self.runner.refresh['guid']
        self.runner.remote_pending[guid]['ts'] -= self.runner.timeout + 1
        self.runner.remote_poll(guid)
        self.assertEqual(self.runner.refresh['status'], 'timeout')
        p = b.Page()
        self.runner.part_index(p, {})
        self.assertIn('keine Antwort von local', str(p))

    def test_index_without_runners(self):
        from nwebclient import base as b
        p = b.Page()
        self.runner.part_index(p, {})
        self.assertIn('Noch keine Runner bekannt', str(p))
        self.assertNotIn('window.location.reload', str(p))

    def test_refresh_shows_runners(self):
        from nwebclient import base as b
        self.runner.extra_runners = ['store']
        self.runner.request_signatures = lambda names: None
        self.runner.execute({'op': 'refresh'})
        self.poll_all()
        p = b.Page()
        self.runner.part_index(p, {})
        self.assertIn('store (?)', str(p))
        self.assertNotIn('Noch keine Runner bekannt', str(p))

    def test_request_signatures(self):
        self.runner.request_signatures(['store'])
        self.poll_all()
        entry = self.runner.remote_runners['store']
        self.assertEqual(entry['error'], '')
        self.assertIn('cenqueue', {sig['job'].get('op') for sig in entry['signatures']})


class FakeSettings:
    """ nweb-Persistence mit setting/set_setting """

    def __init__(self):
        self.values = {}

    def setting(self, key, default=None):
        return self.values.get(key, default)

    def set_setting(self, key, value):
        self.values[key] = value


class RemoteRunnerPersistenceTestCase(unittest.TestCase):

    def create(self, settings):
        runner = TestRemoteRunner(target='pi', store='store')
        root = create_root(js.DirectJobStore(type='store'), runner)
        root.nweb = settings
        return runner

    def test_saves_and_loads_runners(self):
        settings = FakeSettings()
        runner = self.create(settings)
        with runner.remote_lock:
            runner.on_signatures('a', {'success': True, 'signatures': [{'name': 'x'}]})
        self.assertIn('remote_runner.runners.pi', settings.values)
        restarted = self.create(settings)
        res = restarted.execute({'op': 'runners'})
        self.assertTrue(res['persistent'])
        self.assertEqual(res['runners']['a'], {'signatures': [{'name': 'x'}], 'error': ''})

    def test_new_result_wins_over_saved(self):
        settings = FakeSettings()
        settings.set_setting('remote_runner.runners.pi', json.dumps({'a': {'signatures': [], 'error': ''},
                                                                     'b': {'signatures': None, 'error': ''}}))
        runner = self.create(settings)
        with runner.remote_lock:
            runner.on_signatures('a', {'success': True, 'signatures': [{'name': 'neu'}]})
        saved = json.loads(settings.values['remote_runner.runners.pi'])
        self.assertEqual(saved['a']['signatures'], [{'name': 'neu'}])
        self.assertIn('b', saved)

    def test_without_persistence(self):
        runner = self.create(None)
        with runner.remote_lock:
            runner.on_signatures('a', {'success': True, 'signatures': []})
        self.assertFalse(runner.execute({'op': 'runners'})['persistent'])

    def test_invalid_setting(self):
        settings = FakeSettings()
        settings.set_setting('remote_runner.runners.pi', 'kein json')
        runner = self.create(settings)
        self.assertEqual(runner.execute({'op': 'runners'})['runners'], {})


class FmtTsTestCase(unittest.TestCase):

    def test_fmt_ts(self):
        self.assertEqual(js.fmt_ts(None), '')
        self.assertEqual(js.fmt_ts(0), '')
        self.assertRegex(js.fmt_ts(time.time()), r'^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$')


if __name__ == '__main__':
    unittest.main()
