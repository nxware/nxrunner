"""
Unittests fuer nxrunner.mcp (McpSystem)

    python -m unittest tests.test_mcp
"""
import json
import os
import tempfile
import threading
import time
import unittest

from nwebclient import runner as r
from nwebclient import dev as d
from nwebclient.runner import op

from nxrunner.mcp import McpSystem, input_schema, sig_name, tool_name, INVALID_PARAMS, METHOD_NOT_FOUND
from nxrunner.mcp_approval import APPROVED, DENIED, TIMEOUT, REJECTED, kv_table

try:
    import flask  # noqa: F401
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False


class Calc(r.BaseJobExecutor):
    def __init__(self):
        super().__init__('calc')
        self.define_sig(d.PStr('op', 'hello'), d.PStr('who', 'Welt')).set_description('Gruss')

    @op("Addiert zwei Zahlen", result={'success': 'bool', 'sum': 'int'})
    def add(self, a: int, b: int = 1):
        return {'success': True, 'sum': a + b}

    @op("Schlaegt fehl", result={'success': 'bool', 'message': 'str'})
    def broken(self):
        return self.fail('kaputt')

    def execute_hello(self, data):
        return {'success': True, 'message': 'Hallo ' + data.get('who', '')}


class Dispatcher(McpSystem, r.LazyDispatcher):
    pass


def create(args):
    disp = Dispatcher(args=args)
    disp.load_runner('calc', Calc())
    return disp


def rpc(disp, method, params=None, msg_id=1):
    return disp.mcp_handle({'jsonrpc': '2.0', 'id': msg_id, 'method': method, 'params': params or {}})


class McpTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = os.path.join(self.tmp.name, 'mcp.yml')
        self.disp = create({'mcp_enable': 'true', 'mcp_config': self.cfg,
                            'mcp': [{'type': 'calc', 'name': 'add'}, {'type': 'calc', 'name': 'hello'},
                                    {'type': 'calc', 'name': 'broken'}]})

    def tearDown(self):
        self.tmp.cleanup()

    def test_enabled(self):
        self.assertTrue(self.disp.mcp_enabled())
        self.assertFalse(create({}).mcp_enabled())

    def test_names(self):
        sigs = dict(self.disp.mcp_runner_signatures(self.disp.get_runner('calc')))
        self.assertIn('add', sigs)
        self.assertIn('hello', sigs)
        self.assertEqual(sig_name(sigs['hello']), 'hello')
        self.assertEqual(tool_name('a.b', 'c d'), 'a_b__c_d')

    def test_schema(self):
        sigs = dict(self.disp.mcp_runner_signatures(self.disp.get_runner('calc')))
        schema = input_schema(sigs['add'])
        self.assertEqual(schema['properties']['a']['type'], 'integer')
        self.assertEqual(schema['required'], ['a'])
        self.assertNotIn('op', input_schema(sigs['hello'])['properties'])

    def test_initialize(self):
        res = rpc(self.disp, 'initialize', {'protocolVersion': '2025-03-26'}, msg_id=7)
        self.assertEqual(res['id'], 7)
        self.assertEqual(res['result']['protocolVersion'], '2025-03-26')
        self.assertIn('tools', res['result']['capabilities'])

    def test_notification(self):
        self.assertIsNone(self.disp.mcp_handle({'jsonrpc': '2.0', 'method': 'notifications/initialized'}))

    def test_tools_list(self):
        names = [t['name'] for t in rpc(self.disp, 'tools/list')['result']['tools']]
        self.assertEqual(sorted(names), ['calc__add', 'calc__broken', 'calc__hello'])

    def test_not_allowed(self):
        disp = create({'mcp': [{'type': 'calc', 'name': 'add'}]})
        res = rpc(disp, 'tools/call', {'name': 'calc__hello', 'arguments': {}})
        self.assertEqual(res['error']['code'], INVALID_PARAMS)

    def test_call_op(self):
        res = rpc(self.disp, 'tools/call', {'name': 'calc__add', 'arguments': {'a': 2, 'b': 3}})['result']
        self.assertFalse(res['isError'])
        self.assertEqual(res['structuredContent']['sum'], 5)
        self.assertEqual(json.loads(res['content'][0]['text'])['sum'], 5)

    def test_call_define_sig(self):
        res = rpc(self.disp, 'tools/call', {'name': 'calc__hello', 'arguments': {'who': 'MCP'}})['result']
        self.assertEqual(res['structuredContent']['message'], 'Hallo MCP')

    def test_call_fail(self):
        res = rpc(self.disp, 'tools/call', {'name': 'calc__broken', 'arguments': {}})['result']
        self.assertTrue(res['isError'])
        res = rpc(self.disp, 'tools/call', {'name': 'calc__add', 'arguments': {}})['result']
        self.assertTrue(res['isError'])  # a fehlt

    def test_unknown_method(self):
        self.assertEqual(rpc(self.disp, 'foo/bar')['error']['code'], METHOD_NOT_FOUND)

    def test_session_only(self):
        page = self.disp.page_mcp({'mcp_action': 'session', 'sig:calc:hello': 'on'})
        self.assertEqual(self.disp.mcp_allowed(), [{'type': 'calc', 'name': 'hello'}])
        self.assertFalse(os.path.exists(self.cfg))
        self.assertIn('nur diese Sitzung', page)
        names = [t['name'] for t in rpc(self.disp, 'tools/list')['result']['tools']]
        self.assertEqual(names, ['calc__hello'])

    def test_no_action(self):
        self.disp.page_mcp({'sig:calc:hello': 'on'})  # ohne mcp_action keine Aenderung
        self.assertEqual(len(self.disp.mcp_allowed()), 3)

    def test_save(self):
        self.disp.page_mcp({'mcp_action': 'session', 'sig:calc:hello': 'on'})
        page = self.disp.page_mcp({'mcp_action': 'save', 'sig:calc:add': 'on'})
        self.assertEqual(self.disp.mcp_allowed(), [{'type': 'calc', 'name': 'add'}])
        self.assertNotIn('nur diese Sitzung', page)
        with open(self.cfg, encoding='utf-8') as f:
            lines = f.read().splitlines()
        self.assertIn('mcp:', lines)
        entries = [json.loads(line[4:]) for line in lines if line.startswith('  - ')]
        self.assertEqual(entries, [{'type': 'calc', 'name': 'add'}])
        try:
            import yaml
        except ImportError:
            return
        with open(self.cfg, encoding='utf-8') as f:
            self.assertEqual(yaml.safe_load(f)['mcp'], [{'type': 'calc', 'name': 'add'}])

    def test_page_in_nxrunner_ui(self):
        from nxrunner.ui import NxRunner
        disp = NxRunner()
        disp.load_runner('calc', Calc())
        disp._mcp_allowed = [{'type': 'calc', 'name': 'add'}]
        page = disp.page_mcp({})
        self.assertIn('<h1>NxRunner</h1>', page)  # ui.html
        self.assertIn('sig:calc:add', page)
        self.assertIn('claude mcp add', page)

    def test_page_fallback(self):
        page = self.disp.page_mcp({})  # Dispatcher ohne render_ui_page -> nxui
        self.assertIn('sig:calc:add', page)
        self.assertNotIn('<h1>NxRunner</h1>', page)

    def test_without_signatures(self):
        disp = Dispatcher(args={'mcp': []}, calc2='tests.test_mcp:Calc', empty='nwebclient.runner:Echo')
        page = disp.page_mcp({})
        self.assertNotIn('Noch nicht geladen', page)
        self.assertNotIn('sig:calc2:add', page)  # noch nicht erzeugt
        disp.get_runner('empty')
        self.assertIn('ohne benannte Signaturen: empty', disp.page_mcp({}))

    def test_modes_from_page(self):
        self.disp.page_mcp({'mcp_action': 'session', 'sig:calc:add': 'direct', 'sig:calc:hello': 'approve',
                            'sig:calc:broken': ''})
        self.assertEqual(self.disp.mcp_allowed(), [{'type': 'calc', 'name': 'add'},
                                                   {'type': 'calc', 'name': 'hello', 'approve': True}])
        page = self.disp.page_mcp({})
        self.assertRegex(page, r'<input type="radio" value="approve"[^>]*checked="checked"[^>]*name="sig:calc:hello"')
        self.assertNotRegex(page, r'<input type="radio" value="direct"[^>]*checked="checked"[^>]*name="sig:calc:hello"')
        self.assertRegex(page, r'<input type="radio" value=""[^>]*checked="checked"[^>]*name="sig:calc:broken"')
        self.assertNotIn('<select', page)
        tools = {t['name']: t for t in rpc(self.disp, 'tools/list')['result']['tools']}
        self.assertIn('Freigabe', tools['calc__hello']['description'])
        self.assertNotIn('Freigabe', tools['calc__add']['description'])

    def test_save_approve(self):
        self.disp.page_mcp({'mcp_action': 'save', 'sig:calc:hello': 'approve'})
        with open(self.cfg, encoding='utf-8') as f:
            self.assertIn('  - {"type": "calc", "name": "hello", "approve": true}', f.read().splitlines())

    def test_config_approve(self):
        disp = create({'mcp': [{'type': 'calc', 'name': 'add', 'approve': 'true'}]})
        self.assertEqual(disp.mcp_mode('calc', 'add'), 'approve')


def call_in_thread(disp, tool, arguments):
    """ tools/call im Hintergrund, Ergebnis in box[0] """
    box = []
    t = threading.Thread(target=lambda: box.append(rpc(disp, 'tools/call', {'name': tool, 'arguments': arguments})))
    t.start()
    return t, box


def wait_pending(disp, count=1):
    for _ in range(200):
        if len(disp.mcp_approvals().pending()) >= count:
            return disp.mcp_approvals().pending()
        time.sleep(0.01)
    raise AssertionError('keine offene Freigabe')


class McpApprovalTest(unittest.TestCase):

    def setUp(self):
        self.disp = create({'mcp_enable': True, 'mcp_approve_timeout': 5, 'mcp_approve_max': 1,
                            'mcp': [{'type': 'calc', 'name': 'add', 'approve': True},
                                    {'type': 'calc', 'name': 'hello'}]})

    def test_direct_without_approve(self):
        res = rpc(self.disp, 'tools/call', {'name': 'calc__hello', 'arguments': {}})['result']
        self.assertFalse(res['isError'])
        self.assertEqual(self.disp.mcp_approvals().history(), [])

    def test_approve(self):
        t, box = call_in_thread(self.disp, 'calc__add', {'a': 1, 'b': 2})
        pending = wait_pending(self.disp)
        self.assertEqual(pending[0].job, {'a': 1, 'b': 2, 'op': 'add', 'type': 'calc'})
        self.assertIsNone(pending[0].result)
        self.assertIn('Anfrage 1 genehmigt', self.disp.mcp_decide({'approve': str(pending[0].id)}))
        t.join(5)
        self.assertEqual(box[0]['result']['structuredContent']['sum'], 3)
        done = self.disp.mcp_approvals().history()[0]
        self.assertEqual(done.decision, APPROVED)
        self.assertEqual(done.result, {'success': True, 'sum': 3})
        self.assertIsNotNone(done.executed)

    def test_deny(self):
        t, box = call_in_thread(self.disp, 'calc__add', {'a': 1})
        pending = wait_pending(self.disp)
        self.disp.mcp_decide({'deny': str(pending[0].id)})
        t.join(5)
        res = box[0]['result']
        self.assertTrue(res['isError'])
        self.assertEqual(res['structuredContent']['approval'], DENIED)
        self.assertIn('nicht mehr offen', self.disp.mcp_decide({'approve': str(pending[0].id)}))

    def test_timeout(self):
        self.disp.args['mcp_approve_timeout'] = 0.05
        res = rpc(self.disp, 'tools/call', {'name': 'calc__add', 'arguments': {'a': 1}})['result']
        self.assertEqual(res['structuredContent']['approval'], TIMEOUT)
        self.assertEqual(self.disp.mcp_approvals().pending(), [])

    def test_max_pending(self):
        t, box = call_in_thread(self.disp, 'calc__add', {'a': 1})
        wait_pending(self.disp)
        res = rpc(self.disp, 'tools/call', {'name': 'calc__add', 'arguments': {'a': 2}})['result']
        self.assertEqual(res['structuredContent']['approval'], REJECTED)
        self.disp.mcp_decide({'deny': str(self.disp.mcp_approvals().pending()[0].id)})
        t.join(5)

    @unittest.skipUnless(HAS_FLASK, 'flask fehlt')
    def test_page_and_endpoint(self):
        from flask import Flask
        app = Flask(__name__)
        self.disp.setup_mcp(app)
        client = app.test_client()
        t, box = call_in_thread(self.disp, 'calc__add', {'a': 7})
        pending = wait_pending(self.disp)
        self.assertRegex(self.disp.page_mcp({}), r'Offene Freigaben: <span>1 <a href="/sys/mcp/pending"')
        page = client.get('/sys/mcp/pending').get_data(as_text=True)
        self.assertIn('Offene Freigaben (1)', page)
        self.assertIn(f"mcpDecide('approve', {pending[0].id})", page)
        self.assertIn('<b>a</b></td><td><code>7</code>', page)  # Job als Schluessel/Wert-Tabelle
        data = client.get('/sys/mcp/approvals').get_json()
        self.assertEqual(data['count'], 1)
        resp = client.post('/sys/mcp/approvals', data={'approve': pending[0].id},
                           headers={'Origin': 'https://evil.example'})
        self.assertEqual(resp.status_code, 403)
        data = client.post('/sys/mcp/approvals', data={'approve': pending[0].id}).get_json()
        self.assertEqual(data['count'], 0)
        t.join(5)
        self.assertEqual(box[0]['result']['structuredContent']['sum'], 8)
        page = client.get('/sys/mcp/pending').get_data(as_text=True)
        self.assertIn('Letzte Entscheidungen', page)
        self.assertIn('Ergebnis', page)
        self.assertIn('<b>sum</b></td><td><code>8</code>', page)

    @unittest.skipUnless(HAS_FLASK, 'flask fehlt')
    def test_no_cache(self):
        from flask import Flask
        app = Flask(__name__)
        self.disp.setup_mcp(app)
        resp = app.test_client().get('/sys/mcp')
        self.assertEqual(resp.headers.get('Cache-Control'), 'no-store')

    @unittest.skipUnless(HAS_FLASK, 'flask fehlt')
    def test_runner_url_redirect(self):
        from flask import Flask
        app = Flask(__name__)
        self.disp.setup_mcp(app)
        resp = app.test_client().get('/sys/mcp?type=calc&calc=index')
        self.assertEqual(resp.status_code, 302)
        self.assertTrue(resp.headers['Location'].endswith('/pysys/ui?type=calc&calc=index'))

    def test_ui_html_absolute_urls(self):
        from nxrunner.ui import load_resource
        js = load_resource('ui.html')
        self.assertNotIn("'?' + new URLSearchParams", js.replace("UI_URL + '?' + new URLSearchParams", ''))
        self.assertNotIn('href="?', js)
        self.assertNotIn("location.href = '?'", js)

    def test_origin(self):
        self.assertTrue(self.disp.mcp_origin_allowed('http://localhost:3000', 'http://pi:7070/'))
        self.assertTrue(self.disp.mcp_origin_allowed('http://pi:7070', 'http://pi:7070/'))
        self.assertFalse(self.disp.mcp_origin_allowed('https://evil.example', 'http://pi:7070/'))


class KvTableTest(unittest.TestCase):

    def test_values(self):
        res = kv_table({'type': 'calc', 'n': 5, 'flag': True, 'none': None, 'nested': {'x': [1, 2]},
                        'html': '<b>'})
        self.assertIn('<b>type</b></td><td><span style="white-space: pre-wrap;">calc</span>', res)
        self.assertIn('<code>5</code>', res)
        self.assertIn('<code>true</code>', res)
        self.assertIn('<code>null</code>', res)
        self.assertIn('{\n  &quot;x&quot;: [\n    1,\n    2\n  ]\n}', res)  # verschachtelt als JSON
        self.assertIn('&lt;b&gt;', res)
        self.assertNotIn('<b><', res)

    def test_no_dict(self):
        self.assertIn('[\n  1\n]', kv_table([1]))
        self.assertIn('(leer)', kv_table({}))


if __name__ == '__main__':
    unittest.main()
