"""
    MCP-Server (Model Context Protocol) fuer nxrunner: freigeschaltete Runner-Signaturen werden als MCP-Tools angeboten

    Aktiv nur mit --mcp oder mcp_enable: true. Dann gibt es
      /mcp      JSON-RPC-Endpunkt (Streamable HTTP, nur POST mit JSON-Antwort, kein SSE)
      /sys/mcp  Freischalten der Signaturen je Runner und Einbindungsanleitung
      /sys/mcp/pending  Jobs, die auf Freigabe warten (Genehmigen/Ablehnen), und letzte Ergebnisse

    Freigeschaltete Signaturen stehen in der Config:

        mcp:
          - {type: myrunner, name: mymethod}                  # wird direkt ausgefuehrt
          - {type: myrunner, name: danger, approve: true}     # wartet auf Freigabe unter /sys/mcp

    Auf /sys/mcp gilt "Uebernehmen" nur fuer diese Sitzung (bis zum Neustart), "Speichern" schreibt zusaetzlich die Config.

    Weitere Args: mcp_token (Bearer-Token fuer /mcp), mcp_origins (erlaubte Origins, kommagetrennt),
    mcp_config (Datei, in die /sys/mcp speichert, Standard: <Args.CONFIG_D>/mcp.yml),
    mcp_approve_timeout (Sekunden, die ein Aufruf auf Freigabe wartet, Standard 120),
    mcp_approve_max (gleichzeitig wartende Aufrufe, Standard 2, jeder belegt einen Thread des Webservers)
"""
import html
import json
import os
import re
import threading
import traceback

from nwebclient import base
from nwebclient import web as w
from nwebclient import util as u
from nwebclient.runner import op

from nxrunner.mcp_approval import McpApprovals, approvals_html

PROTOCOL_VERSION = '2025-06-18'
PROTOCOL_VERSIONS = ('2025-06-18', '2025-03-26', '2024-11-05')

# JSON-RPC Fehlercodes
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

JSON_TYPES = {'int': 'integer', 'float': 'number', 'bool': 'boolean', 'list': 'array', 'dict': 'object',
              'str': 'string'}

# Parameter, die nicht als Tool-Argument angeboten werden (op ist bei define_sig fest, type setzt der Dispatcher)
SKIP_PARAMS = ('op', 'type')

TRUE_VALUES = ('1', 'true', 'yes', 'on')

# Modus einer Signatur auf /sys/mcp: aus, direkt ausfuehren, erst nach Freigabe
MODE_OFF, MODE_DIRECT, MODE_APPROVE = '', 'direct', 'approve'
MODES = {MODE_OFF: 'aus', MODE_DIRECT: 'direkt', MODE_APPROVE: 'Freigabe'}

_approvals_lock = threading.Lock()

# Freigabe-Bereich auf /sys/mcp/pending: Entscheidungen per fetch, alle 3 s aktualisieren
MCP_APPROVALS_JS = """
window.mcpDecide = function(action, id) {
  const body = new URLSearchParams(); body.set(action, id);
  fetch('/sys/mcp/approvals', {method: 'POST', body: body}).then(r => r.json()).then(mcpShowApprovals);
};
window.mcpShowApprovals = function(data) {
  const el = document.getElementById('mcp_approvals');
  if (el && data && data.html !== undefined && el.innerHTML !== data.html) el.innerHTML = data.html;
};
if (window.mcpApprovalsTimer) clearInterval(window.mcpApprovalsTimer);
window.mcpApprovalsTimer = setInterval(function() {
  if (!document.getElementById('mcp_approvals')) { clearInterval(window.mcpApprovalsTimer); return; }
  fetch('/sys/mcp/approvals', {cache: 'no-store'}).then(r => r.json()).then(mcpShowApprovals).catch(() => {});
}, 3000);
"""


class McpError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def sig_name(sig):
    """ Name einer Signatur: Func-Name (@op, define_sig(self.methode)) oder fester Wert von op (define_sig(PStr('op', 'x'))) """
    if isinstance(sig.name, str) and sig.name != '':
        return sig.name
    for p in sig.items:
        if p.name == 'op' and isinstance(p.default_value, str) and p.default_value != '':
            return p.default_value
    return None


def tool_name(runner_type, name):
    """ MCP-Toolname <type>__<name>, nur [A-Za-z0-9_-], hoechstens 64 Zeichen """
    return re.sub(r'[^A-Za-z0-9_\-]', '_', f'{runner_type}__{name}')[:64]


def json_value(value):
    """ Wert JSON-faehig machen (nicht serialisierbare Werte als Text) """
    return json.loads(json.dumps(value, default=str))


def param_schema(param):
    """ JSON-Schema eines Parameters (dev.Param) """
    schema = {'type': JSON_TYPES.get(str(param.type), 'string')}
    if getattr(param, 'description', ''):
        schema['description'] = param.description
    if param.default_value is not None:
        schema['default'] = json_value(param.default_value)
    return schema


def input_schema(sig):
    """ inputSchema eines Tools aus den Parametern der Signatur """
    props, required = {}, []
    for p in sig.items:
        if p.name in SKIP_PARAMS:
            continue
        props[p.name] = param_schema(p)
        if not getattr(p, '_has_default_value', False) and p.default_value is None:
            required.append(p.name)
    schema = {'type': 'object', 'properties': props}
    if required:
        schema['required'] = required
    return schema


def is_true(value):
    return value is True or str(value).strip().lower() in TRUE_VALUES


class McpSystem:
    """
      Mixin fuer den Dispatcher (NxRunner): MCP-Endpunkt /mcp und Verwaltungsseite /sys/mcp
      Erwartet vom Dispatcher: args, key, instances, get_runner, execute, create_request_params
    """

    MCP_SERVER_NAME = 'nxrunner'

    _mcp_allowed = None  # Liste {type, name[, approve: True]}, None = noch nicht aus der Config gelesen
    _mcp_unsaved = False  # True: Freischaltungen nur fuer diese Sitzung (bis zum Neustart), nicht in der Config
    _mcp_approvals = None  # McpApprovals, beim ersten Zugriff erzeugt

    # --- Config -----------------------------------------------------------------------------

    def mcp_arg(self, name, default=None):
        args = getattr(self, 'args', None)
        if args is None:
            return default
        if isinstance(args, dict):
            return args.get(name, default)
        return args.env(name, default)

    def mcp_enabled(self) -> bool:
        """ MCP nur mit --mcp oder mcp_enable """
        args = getattr(self, 'args', None)
        if args is not None and hasattr(args, 'hasFlag') and args.hasFlag('mcp'):
            return True
        return is_true(self.mcp_arg('mcp_enable', False))

    @staticmethod
    def mcp_entry_of(runner_type, name, approve=False) -> dict:
        """ Eintrag fuer die Config, approve nur wenn gesetzt """
        entry = {'type': str(runner_type), 'name': str(name)}
        if approve:
            entry['approve'] = True
        return entry

    def mcp_allowed(self) -> list:
        """ freigeschaltete Signaturen [{type, name[, approve]}] (Config mcp) """
        if self._mcp_allowed is None:
            value = self.mcp_arg('mcp', None)
            items = value if isinstance(value, list) else []
            self._mcp_allowed = [self.mcp_entry_of(e['type'], e['name'], is_true(e.get('approve', False)))
                                 for e in items if isinstance(e, dict) and 'type' in e and 'name' in e]
        return self._mcp_allowed

    def mcp_mode(self, runner_type, name) -> str:
        """ MODE_OFF, MODE_DIRECT oder MODE_APPROVE """
        for e in self.mcp_allowed():
            if e['type'] == str(runner_type) and e['name'] == str(name):
                return MODE_APPROVE if e.get('approve') else MODE_DIRECT
        return MODE_OFF

    def mcp_is_allowed(self, runner_type, name) -> bool:
        return self.mcp_mode(runner_type, name) != MODE_OFF

    def mcp_config_path(self) -> str:
        return str(self.mcp_arg('mcp_config', None) or os.path.join(u.Args.CONFIG_D, 'mcp.yml'))

    def mcp_set_allowed(self, allowed: list):
        """ Freischaltungen nur fuer diese Sitzung setzen (gelten bis zum Neustart, Config bleibt unveraendert) """
        self._mcp_allowed = list(allowed)
        self._mcp_unsaved = True

    def mcp_save(self, allowed: list) -> str:
        """ Freischaltungen setzen und als mcp: in mcp_config_path() schreiben (JSON-Zeilen sind gueltiges YAML) """
        self._mcp_allowed = list(allowed)
        self._mcp_unsaved = False
        path = self.mcp_config_path()
        lines = ['# Freigeschaltete MCP-Tools, geschrieben von /sys/mcp (nxrunner)', 'mcp:']
        lines += ['  - ' + json.dumps(e) for e in self._mcp_allowed]
        if not self._mcp_allowed:
            lines[-1] = 'mcp: []'
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        with open(path, 'w', encoding='utf-8', newline='\n') as f:
            f.write('\n'.join(lines) + '\n')
        return path

    # --- Tools ------------------------------------------------------------------------------

    def mcp_runner_signatures(self, runner):
        """ [(name, sig)] einer Runner-Instanz, nur Signaturen mit Namen """
        res = []
        for sig in getattr(runner, 'signatures', None) or []:
            name = sig_name(sig)
            if name is not None:
                res.append((name, sig))
        return res

    def mcp_tools_map(self) -> dict:
        """ {toolname: (type, name, sig, approve)} der freigeschalteten Signaturen, Runner werden bei Bedarf erzeugt """
        res = {}
        for entry in self.mcp_allowed():
            try:
                runner = self.get_runner(entry['type'])
            except Exception as e:
                self.error(f"MCP: Runner {entry['type']} nicht erzeugt: {e}")
                continue
            for name, sig in self.mcp_runner_signatures(runner) if runner is not None else []:
                if name == entry['name']:
                    res[tool_name(entry['type'], name)] = (entry['type'], name, sig, bool(entry.get('approve')))
                    break
        return res

    def mcp_tool_def(self, tool, runner_type, name, sig, approve=False) -> dict:
        description = str(sig.description or getattr(sig, 'doc', None) or f'{name} von {runner_type}').strip()
        if approve:
            description += (f' (Erfordert eine Freigabe durch einen Menschen in nxrunner, der Aufruf wartet bis zu '
                            f'{int(self.mcp_approve_timeout())} s.)')
        return {'name': tool, 'title': f'{runner_type}: {name}', 'description': description,
                'inputSchema': input_schema(sig)}

    @op("Liste der freigeschalteten MCP-Tools", result={'success': 'bool', 'message': 'str', 'value': 'list'})
    def mcp_tools(self):
        tools = [self.mcp_tool_def(t, *v) for t, v in self.mcp_tools_map().items()]
        return {'success': True, 'message': f'{len(tools)} Tools', 'value': tools}

    def mcp_call_tool(self, tool, arguments: dict) -> dict:
        """ fuehrt ein Tool als Job aus, Ergebnis im MCP-Format (content, structuredContent, isError) """
        found = self.mcp_tools_map().get(tool)
        if found is None:
            raise McpError(INVALID_PARAMS, 'Unbekanntes Tool: ' + str(tool))
        runner_type, name, sig, approve = found
        known = {p.name for p in sig.items if p.name not in SKIP_PARAMS}
        job = {k: v for k, v in sig.job({k: v for k, v in arguments.items() if k in known}).items() if v is not None}
        job[self.key] = runner_type
        approval = None
        if approve:
            approval = self.mcp_await_approval(tool, job)
            if not approval.approved:
                return self.mcp_denied_result(tool, approval)
        result, mcp_res = self.mcp_execute_job(tool, job)
        if approval is not None:
            self.mcp_approvals().set_result(approval, result)
        return mcp_res

    def mcp_execute_job(self, tool, job: dict):
        """ :return: (Ergebnis fuer die Anzeige, Ergebnis im MCP-Format) """
        try:
            result = self.execute(job)
        except Exception as e:
            self.error(f'MCP {tool}: {e}')
            message = f'{type(e).__name__}: {e}'
            return {'success': False, 'message': message}, {'content': [{'type': 'text', 'text': message}],
                                                            'isError': True}
        return json_value(result), self.mcp_result(result)

    # --- Freigabe ---------------------------------------------------------------------------

    def mcp_approvals(self) -> McpApprovals:
        if self._mcp_approvals is None:
            with _approvals_lock:
                if self._mcp_approvals is None:
                    self._mcp_approvals = McpApprovals(int(self.mcp_arg('mcp_approve_max', 2) or 2))
        return self._mcp_approvals

    def mcp_approve_timeout(self) -> float:
        return float(self.mcp_arg('mcp_approve_timeout', 120) or 120)

    @staticmethod
    def mcp_client() -> str:
        """ IP des Aufrufers (nur im Flask-Request) """
        try:
            from flask import request
            return str(request.remote_addr or '')
        except Exception:
            return ''

    def mcp_await_approval(self, tool, job: dict):
        """ wartet auf die Freigabe des Jobs unter /sys/mcp/pending; :return: Approval (decision gesetzt) """
        self.info(f'MCP: {tool} wartet auf Freigabe')
        approval = self.mcp_approvals().request(tool, job, self.mcp_client(), self.mcp_approve_timeout())
        self.info(f'MCP: {tool} {approval.decision}')
        return approval

    @staticmethod
    def mcp_denied_result(tool, approval) -> dict:
        message = f'Aufruf von {tool} nicht ausgefuehrt: {approval.decision}'
        return {'content': [{'type': 'text', 'text': message}], 'isError': True,
                'structuredContent': {'success': False, 'message': message, 'approval': approval.decision}}

    @staticmethod
    def mcp_result(result) -> dict:
        data = json_value(result if isinstance(result, dict) else {'value': result})
        return {'content': [{'type': 'text', 'text': json.dumps(data, ensure_ascii=False, indent=1)}],
                'structuredContent': data,
                'isError': isinstance(result, dict) and result.get('success') is False}

    # --- JSON-RPC ---------------------------------------------------------------------------

    def mcp_on_initialize(self, params: dict) -> dict:
        requested = params.get('protocolVersion')
        version = requested if requested in PROTOCOL_VERSIONS else PROTOCOL_VERSION
        return {'protocolVersion': version,
                'capabilities': {'tools': {'listChanged': False}},
                'serverInfo': {'name': self.MCP_SERVER_NAME, 'version': self.mcp_version()},
                'instructions': 'Tools von nxrunner-Runnern, Name <runner>__<operation>.'}

    @staticmethod
    def mcp_version():
        try:
            from importlib.metadata import version
            return version('nxrunner')
        except Exception:
            return '0'

    def mcp_on_tools_call(self, params: dict) -> dict:
        arguments = params.get('arguments') or {}
        if not isinstance(arguments, dict):
            raise McpError(INVALID_PARAMS, 'arguments muss ein Objekt sein')
        return self.mcp_call_tool(params.get('name'), arguments)

    def mcp_method(self, method):
        return {'initialize': self.mcp_on_initialize,
                'ping': lambda params: {},
                'tools/list': lambda params: {'tools': self.mcp_tools()['value']},
                'tools/call': self.mcp_on_tools_call}.get(method)

    def mcp_handle(self, msg):
        """ eine JSON-RPC-Nachricht (oder ein Batch) verarbeiten, None bei Notifications/Antworten """
        if isinstance(msg, list):
            res = [r for r in (self.mcp_handle(m) for m in msg) if r is not None]
            return res or None
        if not isinstance(msg, dict) or msg.get('jsonrpc') != '2.0':
            return self.mcp_error(None, INVALID_REQUEST, 'Kein JSON-RPC 2.0')
        if 'method' not in msg:
            return None  # Antwort des Clients
        msg_id, is_notification = msg.get('id'), 'id' not in msg
        try:
            handler = self.mcp_method(msg['method'])
            if handler is None:
                raise McpError(METHOD_NOT_FOUND, 'Unbekannte Methode: ' + str(msg['method']))
            result = handler(msg.get('params') or {})
        except McpError as e:
            return None if is_notification else self.mcp_error(msg_id, e.code, e.message)
        except Exception as e:
            self.error('MCP: ' + traceback.format_exc())
            return None if is_notification else self.mcp_error(msg_id, INTERNAL_ERROR, str(e))
        return None if is_notification else {'jsonrpc': '2.0', 'id': msg_id, 'result': result}

    @staticmethod
    def mcp_error(msg_id, code, message):
        return {'jsonrpc': '2.0', 'id': msg_id, 'error': {'code': code, 'message': message}}

    # --- HTTP -------------------------------------------------------------------------------

    def mcp_check_request(self, request):
        """ Fehlermeldung, (status) oder None: Bearer-Token (mcp_token) und Origin (DNS-Rebinding) pruefen """
        token = self.mcp_arg('mcp_token', None)
        if token and request.headers.get('Authorization', '') != f'Bearer {token}':
            return 'Unauthorized', 401
        origin = request.headers.get('Origin')
        if origin and not self.mcp_origin_allowed(origin, request.host_url):
            return 'Origin nicht erlaubt: ' + origin, 403
        return None

    def mcp_origin_allowed(self, origin, host_url) -> bool:
        allowed = [o.strip().rstrip('/') for o in str(self.mcp_arg('mcp_origins', '') or '').split(',') if o.strip()]
        origin = origin.rstrip('/')
        local = re.match(r'^https?://(localhost|127\.0\.0\.1|\[::1\])(:\d+)?$', origin)
        return bool(local) or origin == host_url.rstrip('/') or origin in allowed

    def mcp_http(self):
        """ /mcp: POST JSON-RPC, Antwort application/json; Notifications 202 """
        from flask import request, Response
        denied = self.mcp_check_request(request)
        if denied is not None:
            return Response(json.dumps(self.mcp_error(None, INVALID_REQUEST, denied[0])), status=denied[1],
                            mimetype='application/json')
        if request.method != 'POST':
            return Response('Nur POST (kein SSE-Stream)', status=405, headers={'Allow': 'POST'})
        try:
            msg = json.loads(request.get_data(as_text=True) or 'null')
        except ValueError as e:
            return Response(json.dumps(self.mcp_error(None, PARSE_ERROR, str(e))), status=400,
                            mimetype='application/json')
        result = self.mcp_handle(msg)
        if result is None:
            return Response('', status=202)
        return Response(json.dumps(result, default=str, ensure_ascii=False), mimetype='application/json')

    def setup_mcp(self, app):
        if not self.mcp_enabled():
            return
        try:
            app.add_url_rule('/mcp', 'nxrunner_mcp', view_func=self.mcp_http, methods=['GET', 'POST', 'DELETE'])
            app.add_url_rule('/sys/mcp', 'nxrunner_sys_mcp',
                             view_func=self.mcp_page_response, methods=['GET', 'POST'])
            app.add_url_rule('/sys/mcp/pending', 'nxrunner_sys_mcp_pending',
                             view_func=self.mcp_pending_response, methods=['GET'])
            app.add_url_rule('/sys/mcp/approvals', 'nxrunner_sys_mcp_approvals',
                             view_func=self.mcp_approvals_response, methods=['GET', 'POST'])
            self.info('MCP-Server unter /mcp, Verwaltung unter /sys/mcp')
        except Exception as e:
            self.error('MCP nicht eingerichtet: ' + str(e))

    # --- Seite /sys/mcp -----------------------------------------------------------------------

    @classmethod
    def mcp_allowed_from_params(cls, params: dict) -> list:
        """ Auswahl sig:<type>:<name> = direct|approve (bzw. on bei Checkbox) -> [{type, name[, approve]}] """
        allowed = []
        for key, value in params.items():
            if key.startswith('sig:') and key.count(':') >= 2 and str(value) not in (MODE_OFF, 'off'):
                _, runner_type, name = key.split(':', 2)
                allowed.append(cls.mcp_entry_of(runner_type, name, str(value) == MODE_APPROVE))
        return allowed

    def mcp_apply_params(self, p: base.Page, params: dict):
        """ mcp_action=session: nur fuer diese Sitzung, mcp_action=save: zusaetzlich in die Config schreiben """
        action = params.get('mcp_action')
        allowed = self.mcp_allowed_from_params(params)
        if action == 'session':
            self.mcp_set_allowed(allowed)
            p.div(w.alert_success(f'{len(allowed)} Signaturen fuer diese Sitzung freigeschaltet (bis zum Neustart, '
                                  f'nicht gespeichert)'))
        elif action == 'save':
            try:
                path = self.mcp_save(allowed)
                p.div(w.alert_success(f'{len(allowed)} Signaturen freigeschaltet, gespeichert in {html.escape(path)}'))
            except OSError as e:
                self.mcp_set_allowed(allowed)
                p.div(w.alert_error('Nicht gespeichert, gilt nur fuer diese Sitzung: ' + html.escape(str(e))))

    def page_mcp(self, params: dict):
        p = base.Page(owner=self)
        self.mcp_apply_params(p, params)
        p.h2('MCP-Server')
        p.prop('Endpunkt', html.escape(self.mcp_url()))
        p.prop('Freigeschaltet', str(len(self.mcp_allowed())) +
               (' (nur diese Sitzung, nicht gespeichert)' if self._mcp_unsaved else ''))
        p.prop('Config', html.escape(self.mcp_config_path()))
        count = self.mcp_approvals().open_count()
        p.prop('Offene Freigaben', f'{count} ' + w.a('anzeigen', '/sys/mcp/pending',
                                                      hint='Jobs, die auf Freigabe warten, und letzte Ergebnisse'))
        self.part_mcp_signatures(p)
        self.part_mcp_guide(p)
        return self.mcp_render(p)

    def page_mcp_pending(self):
        """ /sys/mcp/pending: Jobs, die auf Freigabe warten, und die letzten Entscheidungen mit Ergebnis """
        p = base.Page(owner=self)
        p.h2('MCP-Freigaben')
        p.prop('Wartezeit', f'{int(self.mcp_approve_timeout())} s, danach wird der Aufruf abgebrochen')
        p.div(w.a('Zur MCP-Verwaltung', '/sys/mcp', hint='Signaturen freischalten (aus / direkt / Freigabe)'))
        self.part_mcp_approvals(p)
        return self.mcp_render(p, 'MCP-Freigaben')

    def mcp_html_response(self, page_fn):
        """
          Seite ohne Cache, damit nachgeladene Runner auch nach Zurueck/Neuladen erscheinen.
          ?type=x (Runner-Auswahl der Oberflaeche) -> /pysys/ui?type=x
        """
        from flask import make_response, redirect, request
        if request.method == 'GET' and request.args.get(self.key):
            return redirect('/pysys/ui?' + request.query_string.decode('utf-8', errors='replace'))
        resp = make_response(page_fn())
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    def mcp_page_response(self):
        return self.mcp_html_response(lambda: self.page_mcp(self.create_request_params()))

    def mcp_pending_response(self):
        return self.mcp_html_response(self.page_mcp_pending)

    def mcp_render(self, p: base.Page, name='MCP'):
        """ in der neuen nxrunner-Oberflaeche (NxRunner.render_ui_page), sonst als nxui-Seite """
        render = getattr(self, 'render_ui_page', None)
        return render(name, p) if render is not None else p.nxui()

    def mcp_url(self) -> str:
        try:
            from flask import request
            return request.host_url.rstrip('/') + '/mcp'
        except Exception:
            return 'http://localhost:7070/mcp'

    # --- Freigaben auf /sys/mcp ---------------------------------------------------------------

    def mcp_decide(self, params: dict) -> str:
        """ approve=<id> oder deny=<id>; :return: Meldung """
        approve = 'approve' in params
        approval_id = params.get('approve' if approve else 'deny')
        if approval_id is None or not str(approval_id).isdigit():
            return ''
        if self.mcp_approvals().decide(int(approval_id), approve):
            return f'Anfrage {approval_id} ' + ('genehmigt' if approve else 'abgelehnt')
        return f'Anfrage {approval_id} ist nicht mehr offen'

    def mcp_approvals_response(self):
        """ /sys/mcp/approvals: POST entscheidet (approve/deny=<id>), liefert {count, html} des Freigabe-Bereichs """
        from flask import request, jsonify
        if request.method == 'POST':
            origin = request.headers.get('Origin')
            if origin and not self.mcp_origin_allowed(origin, request.host_url):
                return jsonify({'success': False, 'message': 'Origin nicht erlaubt'}), 403
        message = self.mcp_decide(self.create_request_params()) if request.method == 'POST' else ''
        resp = jsonify({'success': True, 'message': message, 'count': self.mcp_approvals().open_count(),
                        'html': self.mcp_approvals_html(message)})
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    def part_mcp_approvals(self, p: base.Page):
        """ offene Anfragen und Verlauf, aktualisiert sich per /sys/mcp/approvals alle 3 s """
        p.div(self.mcp_approvals_html(), id='mcp_approvals')
        p(w.tag('script', MCP_APPROVALS_JS))

    def mcp_approvals_html(self, message='') -> str:
        return approvals_html(self.mcp_approvals(), message)

    # --- Signaturen auf /sys/mcp ----------------------------------------------------------------

    def part_mcp_signatures(self, p: base.Page):
        """ Formular mit allen geladenen Runnern """
        p('<form method="post" action="/sys/mcp">')
        p.div(self.part_mcp_buttons())
        without = []
        for runner_type, runner in sorted(self.instances.items()):
            if not self.part_mcp_runner(p, runner_type, runner):
                without.append(runner_type)
        if without:
            p.div('Geladen, ohne benannte Signaturen: ' + html.escape(', '.join(without)))
        p('</form>')

    def part_mcp_buttons(self) -> str:
        return ' '.join([
            self.mcp_mode_button('Alle direkt', MODE_DIRECT, 'Alle Signaturen ohne Freigabe freischalten'),
            self.mcp_mode_button('Alle mit Freigabe', MODE_APPROVE, 'Alle Signaturen nur mit Freigabe freischalten'),
            self.mcp_mode_button('Keine', MODE_OFF, 'Alle Signaturen abschalten'),
            w.tag('button', '&Uuml;bernehmen', type='submit', name='mcp_action', value='session',
                  title='Auswahl sofort anwenden, nur fuer diese Sitzung (bis zum Neustart), ohne zu speichern'),
            w.tag('button', 'Speichern', type='submit', name='mcp_action', value='save',
                  title='Auswahl sofort anwenden und als mcp: in die Config schreiben'),
            w.tag('button', 'Aktualisieren', type='button', onclick="location.href='/sys/mcp'",
                  title='Runner-Liste neu einlesen (z.B. nach dem Nachladen eines Runners), Auswahl wird verworfen')])

    @staticmethod
    def mcp_mode_button(label: str, mode: str, hint: str) -> str:
        """ Button (type=button, schickt das Formular nicht ab), der bei allen Signaturen den Radio-Button mode waehlt """
        js = f"document.querySelectorAll('.mcp_sig').forEach(function(c) {{ c.checked = c.value === '{mode}'; }});"
        return w.tag('button', label, type='button', onclick=js, title=hint)

    def part_mcp_runner(self, p: base.Page, runner_type, runner) -> bool:
        """ :return: False, wenn der Runner keine benannten Signaturen hat (nichts ausgegeben) """
        sigs = self.mcp_runner_signatures(runner)
        if not sigs:
            return False
        active = sum(1 for name, _ in sigs if self.mcp_is_allowed(runner_type, name))
        with p.expandable(f'{html.escape(runner_type)} ({active}/{len(sigs)})', expanded=active > 0):
            rows = ''.join(self.mcp_sig_row(runner_type, name, sig) for name, sig in sigs)
            head = ''.join(w.tag('th', t) for t in ('MCP', 'Name', 'Parameter', 'Beschreibung'))
            p.div(w.table(w.tr(head) + rows))
        return True

    MODE_HINTS = {MODE_OFF: 'nicht als MCP-Tool anbieten', MODE_DIRECT: 'Aufruf wird sofort ausgefuehrt',
                  MODE_APPROVE: 'Aufruf wartet auf Freigabe unter /sys/mcp/pending'}

    def mcp_mode_radios(self, runner_type, name) -> str:
        """ Radio-Buttons aus / direkt / Freigabe, eine Gruppe je Signatur (name sig:<type>:<name>) """
        current = self.mcp_mode(runner_type, name)
        group = html.escape(f'sig:{runner_type}:{name}', quote=True)
        tool = tool_name(runner_type, name)
        radios = []
        for mode, label in MODES.items():
            attrs = {'type': 'radio', 'name': group, 'value': mode, '_class': 'mcp_sig'}
            if mode == current:
                attrs['checked'] = 'checked'
            radios.append(w.tag('label', w.input(attrs.pop('name'), **attrs) + ' ' + label,
                                title=f'{tool}: {self.MODE_HINTS[mode]}'))
        return w.tag('span', ' '.join(radios), style='white-space: nowrap;')

    def mcp_sig_row(self, runner_type, name, sig) -> str:
        params = ', '.join(f'{p.name}: {p.type}' for p in sig.items if p.name not in SKIP_PARAMS)
        return w.tr(w.td(self.mcp_mode_radios(runner_type, name), style='white-space: nowrap;') +
                    w.td(html.escape(name)) +
                    w.td(html.escape(params)) + w.td(html.escape(str(sig.description or ''))))

    def part_mcp_guide(self, p: base.Page):
        url = self.mcp_url()
        token = self.mcp_arg('mcp_token', None)
        header = ' --header "Authorization: Bearer <mcp_token>"' if token else ''
        desktop = {'mcpServers': {'nxrunner': {'command': 'npx', 'args': ['-y', 'mcp-remote', url] +
                                               (['--header', 'Authorization: Bearer <mcp_token>'] if token else [])}}}
        with p.section(h='Einbindung'):
            p.p('Claude Code:')
            p.pre(html.escape(f'claude mcp add --transport http nxrunner {url}{header}'))
            p.p('Claude Desktop und Clients ohne HTTP-Transport (claude_desktop_config.json, ueber mcp-remote):')
            p.pre(html.escape(json.dumps(desktop, indent=2)))
            p.p('Test per curl:')
            auth = ' -H "Authorization: Bearer <mcp_token>"' if token else ''
            p.pre(html.escape(f'curl -s -X POST {url} -H "Content-Type: application/json"{auth} '
                              '-d \'{"jsonrpc":"2.0","id":1,"method":"tools/list"}\''))
            p.p('Config (nweb.yml bzw. ' + html.escape(self.mcp_config_path()) + '):')
            p.pre(html.escape('mcp_enable: true          # oder Start mit --mcp\n'
                              'mcp_token: geheim         # optional: Bearer-Token fuer /mcp\n'
                              'mcp_origins: https://x.y  # optional: weitere erlaubte Origins\n'
                              'mcp:\n'
                              '  - {type: myrunner, name: mymethod}'))
            p.p('Toolnamen: &lt;runner&gt;__&lt;signatur&gt;. Freigeschaltete Tools fuehren Jobs mit den Rechten '
                'von nxrunner aus, daher nur benoetigte Signaturen freischalten.')
