"""
Steuerung einer Webseite im Browser.

Der Runner startet einen eigenen Webserver (Standard-Port 7011). Ein Userscript (Tampermonkey/Violentmonkey,
http://<host>:7011/website.user.js) wird in jede Seite des Browsers eingebunden, meldet sich per Long-Polling
beim Server und fuehrt dessen Befehle in der Seite aus.

    Config (runners):
        website: nxrunner.web:WebsiteController()
        website: nxrunner.web:WebsiteController(port=7011)
        website: nxrunner.web:WebsiteController(host="0.0.0.0")    aus dem Netz erreichbar (z.B. in Docker)

    Jobs:
        {"type": "website", "op": "clients"}                                    verbundene Tabs
        {"type": "website", "op": "query", "selector": "h2"}                    Elemente zum CSS-Selektor
        {"type": "website", "op": "highlight", "selector": "h2"}                Elemente in der Seite markieren
        {"type": "website", "op": "extract", "base": "tr", "fields": [...]}     Tabelle: je Basis-Element eine Zeile
        {"type": "website", "op": "apply", "base": "tr", "fields": [...], "changes": [...]}   Werte zurueckschreiben
        {"type": "website", "op": "eval_js", "code": "document.title"}          JavaScript im Tab ausfuehren
        {"type": "website", "op": "click", "selector": "button.next"}

    fields: [{"name": "titel", "selector": "td:nth-child(1)", "attr": ""}, ...]
        selector relativ zum Basis-Element (leer = Basis-Element selbst, "input[1]" = zweiter Treffer, "[0]" = erstes Kind),
        attr: leer = Text bzw. value bei Eingabefeldern, "html", "text" oder ein Attribut wie "href"
    changes: [{"row": 0, "field": "titel", "value": "neu"}, ...]

    Server (Port 7011):
        GET  /website.user.js       Userscript
        POST /api/poll              {client, url, title} -> {commands: [...]}, wartet bis zu 25 s
        POST /api/result            {client, id, result}
"""
import html
import itertools
import json
import queue
import threading
import time
from importlib import resources

from nwebclient import base
from nwebclient import runner as r
from nwebclient.runner import op


def resource(name) -> str:
    return resources.files('nxrunner').joinpath('resources').joinpath(name).read_text(encoding='utf-8')


class Client:
    """ Ein Tab im Browser, in dem das Userscript laeuft """

    def __init__(self, id):
        self.id = id
        self.url = ''
        self.title = ''
        self.last_seen = 0
        self.commands = queue.Queue()

    def to_dict(self):
        return {'client': self.id, 'url': self.url, 'title': self.title, 'last_seen': round(time.time() - self.last_seen, 1)}


class WebsiteController(r.BaseJobExecutor):

    MODULES = ['flask']

    POLL_WAIT = 25      # Sekunden, die /api/poll auf Befehle wartet
    CLIENT_TIMEOUT = 40  # Sekunden ohne Poll, nach denen ein Tab als getrennt gilt

    def __init__(self, type='website', port=7011, host='127.0.0.1', timeout=10):
        """
        :param port: Port des Webservers fuer das Userscript
        :param host: Adresse, an die der Server gebunden wird, Standard nur lokal (0.0.0.0 = alle Schnittstellen)
        :param timeout: Sekunden, die ein Job auf die Antwort der Seite wartet
        """
        super().__init__(type)
        self.port = int(port)
        self.host = str(host)
        self.timeout = float(timeout)
        self.clients: dict[str, Client] = {}
        self.pending = {}
        self.ids = itertools.count(1)
        self.lock = threading.Lock()
        self.server = None
        self.server_error = ''
        self.define_vars('port', 'host', 'timeout')
        self.start_server()

    # --- Server -------------------------------------------------------------------------------

    def create_app(self):
        import flask
        app = flask.Flask('nxrunner.web')

        @app.after_request
        def cors(res):
            res.headers['Access-Control-Allow-Origin'] = '*'
            res.headers['Access-Control-Allow-Headers'] = 'Content-Type'
            # Chrome: Zugriff von oeffentlichen Seiten auf localhost (Private Network Access)
            res.headers['Access-Control-Allow-Private-Network'] = 'true'
            return res

        @app.route('/')
        def index():
            url = flask.request.host_url + 'website.user.js'
            return (f'<h1>nxrunner WebsiteController</h1><p>Userscript installieren: <a href="{html.escape(url)}">'
                    f'{html.escape(url)}</a></p><p>Verbundene Tabs: {len(self.active_clients())}</p>')

        @app.route('/website.user.js')
        def userscript():
            res = flask.Response(self.userscript(flask.request.host_url.rstrip('/')), mimetype='text/javascript')
            res.headers['Cache-Control'] = 'no-cache'
            return res

        @app.route('/api/poll', methods=['POST', 'OPTIONS'])
        def poll():
            if flask.request.method == 'OPTIONS':
                return ''
            return flask.jsonify(commands=self.poll(flask.request.get_json(force=True, silent=True) or {}))

        @app.route('/api/result', methods=['POST', 'OPTIONS'])
        def result():
            if flask.request.method == 'OPTIONS':
                return ''
            data = flask.request.get_json(force=True, silent=True) or {}
            self.receive(data.get('id'), data.get('result', {}))
            return flask.jsonify(success=True)

        return app

    def start_server(self):
        if self.server is not None:
            return
        try:
            from werkzeug.serving import make_server
            self.server = make_server(self.host, self.port, self.create_app(), threaded=True)
        except Exception as e:
            self.server = None
            self.server_error = f'{type(e).__name__}: {e}'
            self.error(f'WebsiteController: Server auf {self.host}:{self.port} nicht gestartet: {self.server_error}')
            return
        self.server_error = ''
        threading.Thread(target=self.server.serve_forever, daemon=True, name=f'{self.type}-server').start()

    def stop_server(self):
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
            self.server = None

    def userscript(self, server_url):
        host = server_url.split('://', 1)[-1].split(':', 1)[0]
        return resource('website.user.js').replace('__SERVER__', server_url).replace('__HOST__', host)

    # --- Kommunikation mit den Tabs -----------------------------------------------------------

    def poll(self, data) -> list:
        """ Long-Polling des Userscripts: wartet auf Befehle fuer den Tab """
        cid = str(data.get('client', ''))
        with self.lock:
            client = self.clients.setdefault(cid, Client(cid))
        client.url = data.get('url', client.url)
        client.title = data.get('title', client.title)
        client.last_seen = time.time()
        commands = []
        try:
            commands.append(client.commands.get(timeout=self.POLL_WAIT))
            while True:
                commands.append(client.commands.get_nowait())
        except queue.Empty:
            pass
        client.last_seen = time.time()
        return commands

    def receive(self, id, result):
        with self.lock:
            pending = self.pending.get(id)
        if pending is not None:
            pending['result'] = result
            pending['event'].set()

    def active_clients(self) -> list:
        limit = time.time() - self.CLIENT_TIMEOUT
        with self.lock:
            for cid in [cid for cid, c in self.clients.items() if c.last_seen < limit]:
                del self.clients[cid]
            return sorted(self.clients.values(), key=lambda c: c.last_seen, reverse=True)

    def find_client(self, client='') -> Client:
        clients = self.active_clients()
        if len(clients) == 0:
            raise RuntimeError(f'Kein Tab verbunden (Userscript von http://<host>:{self.port}/website.user.js installiert?)')
        if not client:
            return clients[0]
        for c in clients:
            if c.id == client:
                return c
        raise RuntimeError(f'Tab {client} nicht verbunden')

    def send(self, cmd, client='', **kwargs) -> dict:
        """ Befehl an einen Tab senden und auf das Ergebnis warten """
        c = self.find_client(client)
        id = next(self.ids)
        pending = {'event': threading.Event(), 'result': None}
        with self.lock:
            self.pending[id] = pending
        try:
            c.commands.put({'id': id, 'cmd': cmd, **kwargs})
            if not pending['event'].wait(self.timeout):
                raise RuntimeError(f'Keine Antwort vom Tab {c.id} ({c.url})')
            return {**pending['result'], 'client': c.id, 'url': c.url}
        finally:
            with self.lock:
                self.pending.pop(id, None)

    def call(self, cmd, client='', **kwargs):
        try:
            return self.send(cmd, client, **kwargs)
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')

    @staticmethod
    def parse_list(value) -> list:
        """ Liste oder JSON-Text einer Liste (ueber REST kommen Parameter je nach nwebclient-Version als Text) """
        if value is None or value == '':
            return []
        if isinstance(value, str):
            value = json.loads(value)
        if not isinstance(value, list):
            raise ValueError(f'Liste erwartet: {value!r}')
        return value

    @classmethod
    def parse_fields(cls, fields) -> list:
        """ fields als Liste von dicts oder Liste von Strings "name" (Basis-Element) """
        result = []
        for f in cls.parse_list(fields):
            if isinstance(f, str):
                f = {'name': f}
            name = str(f.get('name', '')).strip()
            if name:
                result.append({'name': name, 'selector': str(f.get('selector') or '').strip(), 'attr': str(f.get('attr') or '').strip()})
        return result

    # --- Jobs ---------------------------------------------------------------------------------

    @op("Verbundene Tabs", result={'success': 'bool', 'value': 'list'})
    def clients(self):
        return self.success(value=[c.to_dict() for c in self.active_clients()], server=self.server is not None)

    @op("Elemente zum CSS-Selektor", result={'success': 'bool', 'count': 'int', 'items': 'list'})
    def query(self, selector: str = None, client: str = '', limit: int = 200):
        return self.call('query', client, selector=selector, limit=limit)

    @op("Elemente zum CSS-Selektor in der Seite markieren (leer = Markierung entfernen)", result={'success': 'bool', 'count': 'int'})
    def highlight(self, selector: str = '', client: str = ''):
        return self.call('highlight', client, selector=selector)

    @op("Strukturierte Daten: je Basis-Element eine Zeile, je Feld eine Spalte", result={'success': 'bool', 'columns': 'list', 'rows': 'list'})
    def extract(self, base: str = None, fields: list = None, client: str = ''):
        try:
            fields = self.parse_fields(fields)
        except ValueError as e:
            return self.fail(f'fields: {e}')
        if len(fields) == 0:
            return self.fail('Keine Felder angegeben')
        return self.call('extract', client, base=base, fields=fields)

    @op("Geaenderte Werte in die Seite schreiben (changes: [{row, field, value}])", result={'success': 'bool', 'applied': 'int', 'errors': 'list'})
    def apply(self, base: str = None, fields: list = None, changes: list = None, client: str = ''):
        try:
            fields, changes = self.parse_fields(fields), self.parse_list(changes)
        except ValueError as e:
            return self.fail(f'fields/changes: {e}')
        return self.call('apply', client, base=base, fields=fields, changes=changes)

    @op("JavaScript im Tab ausfuehren, Ergebnis der letzten Anweisung (Promises werden abgewartet)", result={'success': 'bool', 'value': 'any', 'type': 'str'})
    def eval_js(self, code: str = None, client: str = ''):
        return self.call('js', client, code=code)

    @op("Element klicken", result={'success': 'bool'})
    def click(self, selector: str = None, index: int = 0, client: str = ''):
        return self.call('click', client, selector=selector, index=index)

    @op("Webserver fuer das Userscript neu starten", result={'success': 'bool', 'message': 'str'})
    def restart_server(self):
        self.stop_server()
        self.start_server()
        if self.server is None:
            return self.fail(self.server_error)
        return self.success(f'Server auf {self.host}:{self.port}')

    # --- Seite --------------------------------------------------------------------------------

    def part_index(self, p: base.Page, params={}):
        with p.section(h='Webseite steuern'):
            if self.server is None:
                p.alert_error(f'Server auf {self.host}:{self.port} läuft nicht: {html.escape(self.server_error)}')
                p(self.action_btn({'title': 'Server starten', 'type': self.type, 'op': 'restart_server'}))
            p.div(f'Userscript (Tampermonkey): <a id="ws_script" href="#" target="_blank"></a>')
            p.div('Tab: <select id="ws_client"></select> <button type="button" id="ws_reload_clients">Aktualisieren</button> '
                  '<button type="button" id="ws_open_tab">Tab öffnen</button>')
        with p.section(h='CSS-Selektor'):
            p('<div><input id="ws_selector" placeholder="z.B. h2, a[href], table tr" style="width: 400px"> '
              '<button type="button" id="ws_query">Abfragen</button> '
              '<button type="button" id="ws_highlight">Markieren</button> '
              '<button type="button" id="ws_unhighlight">Markierung entfernen</button></div>')
            p.div('', id='ws_query_result')
        with p.section(h='Strukturierte Daten'):
            p('<div><input id="ws_base" placeholder="Basis-Selektor, z.B. table tbody tr" style="width: 400px"></div>')
            p('<table id="ws_fields"><thead><tr><th>Name</th><th>Selektor (relativ, leer = Basis, input[1] = 2. Treffer)</th>'
              '<th>Attribut (leer = Text/value, html, href, ...)</th><th></th></tr></thead><tbody></tbody></table>')
            p('<div><button type="button" id="ws_add_field">+ Feld</button> '
              '<button type="button" id="ws_extract">Laden</button> '
              '<button type="button" id="ws_apply" disabled>Änderungen zur Webseite übertragen</button></div>')
            p.div('', id='ws_data_result')
        p.script(resource('website.ui.js').replace('__TYPE__', json.dumps(self.type)).replace('__PORT__', str(self.port))
                 .replace('__TAB_LINK__', json.dumps(self.link(self.part_tab))))

    def part_tab(self, p: base.Page, params={}):
        """ Ein Tab: Infos und JavaScript ausfuehren """
        cid = params.get('client', '')
        try:
            c = self.find_client(cid)
        except RuntimeError as e:
            p.alert_error(html.escape(str(e)))
            c = None
        with p.section(h='Tab ' + html.escape(cid)):
            if c is not None:
                p.div('Titel: ' + html.escape(c.title or '(ohne Titel)'))
                p.div('URL: <a href="' + html.escape(c.url, quote=True) + '" target="_blank">' + html.escape(c.url) + '</a>')
            p.div('<a href="' + html.escape(self.link(self.part_index), quote=True) + '">Zurück</a>')
        with p.section(h='JavaScript ausführen'):
            p('<textarea id="ws_code" rows="8" style="width: 100%; font-family: monospace" '
              'placeholder="document.title&#10;document.querySelectorAll(\'a\').length&#10;await fetch(location.href).then(r => r.status)"></textarea>')
            p('<div><button type="button" id="ws_run">Ausführen</button> <small>Strg+Enter, Ergebnis ist der Wert der letzten '
              'Anweisung, mit <code>await</code>: ein Ausdruck oder Ergebnis per <code>return</code></small></div>')
            p.div('', id='ws_js_result')
        p.script(resource('website.tab.js').replace('__TYPE__', json.dumps(self.type)).replace('__CLIENT__', json.dumps(cid)))
