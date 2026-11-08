"""
LogFoundation: nimmt Log-Eintraege entgegen, haelt die letzten logmax im Speicher und stellt sie dar.

    Config (runners):
        log: nxrunner.system:LogFoundation

    Eintrag: text, created (Unix-Zeit), category, importance (verbose|debug|info|warning|error|alert)

    Job:
        {"type": "log", "op": "log", "text": "...", "category": "app", "importance": "info"}
        {"type": "log", "op": "entries", "category": "ssh-raspi5", "importance": "warning", "limit": 100}
        {"type": "log", "op": "source_start", "id": 0}
        {"type": "log", "op": "source_stop", "id": 0}

    Args (z.B. nweb.yml), Schluessel 'logsources', Liste von Quellen:
        logsources:
          - type: ssh
            host: mysshhost                       # -> category ssh-mysshhost
            file: /var/log/apache2/error.log      # tail -F
            sudo: true                            # optional, default false (sudo -n)
          - type: ssh
            host: mysshhost
            cmd: pm2 logs 2
          - type: mqtt
            host: 127.0.0.1                       # -> category mqtt-127.0.0.1
            port: 1883                            # optional
            topic: logs/#
          - type: cmd
            cmd: journalctl -f                    # -> category cmd-journalctl

    Optional je Quelle: category (statt der automatischen), importance (fest statt aus dem Text erkannt),
    autostart: true (beim Laden starten). Sonst werden Quellen in der GUI gestartet und gestoppt.

    Events: on_info (importance info), on_error (importance error), on_alert (importance alert), Werte wie ein Eintrag.
"""
import collections
import html
import json
import re
import shlex
import subprocess
import threading
import time

from nwebclient import runner as r
from nwebclient import base
from nwebclient import util as u
from nwebclient import web as w
from nwebclient.runner import op


IMPORTANCES = ['verbose', 'debug', 'info', 'warning', 'error', 'alert']

# Erkennung der importance aus dem Text, erste passende Regel gewinnt
IMPORTANCE_PATTERNS = [
    ('alert', re.compile(r'\b(alert|emerg(ency)?|crit(ical)?|fatal|panic)\b', re.I)),
    ('error', re.compile(r'\b(error|err|exception|traceback|failed|failure)\b', re.I)),
    ('warning', re.compile(r'\b(warn(ing)?)\b', re.I)),
    ('debug', re.compile(r'\b(debug)\b', re.I)),
    ('verbose', re.compile(r'\b(verbose|trace)\b', re.I)),
]


def esc(value):
    return html.escape('' if value is None else str(value), quote=True)


def as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on', 'ja')


def fmt_time(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts)) if ts else '-'


def normalize_importance(value, default='info'):
    value = str(value or '').strip().lower()
    aliases = {'warn': 'warning', 'err': 'error', 'critical': 'alert', 'crit': 'alert', 'fatal': 'alert',
               'trace': 'verbose'}
    value = aliases.get(value, value)
    return value if value in IMPORTANCES else default


def guess_importance(text, default='info'):
    for importance, pattern in IMPORTANCE_PATTERNS:
        if pattern.search(text):
            return importance
    return default


class LogSource:
    """ Quelle fuer Log-Eintraege (Eintrag aus dem Arg logsources), wird gestartet und gestoppt """

    def __init__(self, sid, conf: dict, sink):
        """ :param sink: fn(text, category, importance=None) """
        self.id = sid
        self.conf = dict(conf)
        self.kind = str(conf.get('type', conf.get('kind', ''))).strip().lower()
        self.sink = sink
        self.category = str(conf.get('category') or self.default_category())
        self.importance = normalize_importance(conf['importance'], None) if conf.get('importance') else None
        self.autostart = as_bool(conf.get('autostart', False))
        self.running = False
        self.started = None
        self.error = ''
        self.lines = 0

    def default_category(self):
        return self.kind

    def description(self):
        return json.dumps({k: v for k, v in self.conf.items() if k not in ('type', 'kind')})

    def emit(self, text):
        text = text.rstrip('\r\n')
        if text.strip() == '':
            return
        self.lines += 1
        self.sink(text, self.category, self.importance)

    def start(self):
        if self.running:
            return
        self.error = ''
        self.running = True
        self.started = time.time()
        try:
            self.do_start()
        except Exception as e:
            self.running = False
            self.error = str(e)
            raise

    def stop(self):
        self.running = False
        self.do_stop()

    def do_start(self):
        raise NotImplementedError()

    def do_stop(self):
        pass

    def to_dict(self):
        return {'id': self.id, 'type': self.kind, 'category': self.category, 'running': self.running,
                'started': self.started, 'error': self.error, 'lines': self.lines, 'conf': self.conf}


class ProcessLogSource(LogSource):
    """ Quelle, deren Zeilen aus der Ausgabe (stdout + stderr) eines Prozesses kommen """

    def __init__(self, sid, conf, sink):
        super().__init__(sid, conf, sink)
        self.process = None

    def command(self):
        """ :return: Argumentliste oder str (Shell) """
        raise NotImplementedError()

    def do_start(self):
        cmd = self.command()
        self.process = subprocess.Popen(cmd, shell=isinstance(cmd, str), stdin=subprocess.DEVNULL,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        universal_newlines=True, encoding='utf-8', errors='replace', bufsize=1)
        threading.Thread(target=self.read_loop, args=(self.process,), daemon=True,
                         name=f'log-{self.category}').start()

    def read_loop(self, process):
        for line in process.stdout:
            self.emit(line)
        code = process.wait()
        if self.running and process is self.process:
            self.running = False
            self.error = f"Prozess beendet (Exit-Code {code})"
            self.sink(f"Logquelle {self.category} beendet (Exit-Code {code})", 'log', 'warning')

    def do_stop(self):
        process, self.process = self.process, None
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


class CmdLogSource(ProcessLogSource):
    """ type: cmd, cmd: Befehl (lokal, Shell) """

    def default_category(self):
        cmd = str(self.conf.get('cmd', '')).split()
        name = re.split(r'[\\/]', cmd[0])[-1].rsplit('.', 1)[0] if cmd else 'cmd'
        return 'cmd-' + name

    def command(self):
        cmd = str(self.conf.get('cmd', '')).strip()
        if not cmd:
            raise ValueError("cmd fehlt")
        return cmd


class SshLogSource(ProcessLogSource):
    """ type: ssh, host, file [sudo] oder cmd """

    def default_category(self):
        return 'ssh-' + str(self.conf.get('host', ''))

    def remote_cmd(self):
        if self.conf.get('file'):
            cmd = 'tail -n 0 -F ' + shlex.quote(str(self.conf['file']))
        elif self.conf.get('cmd'):
            cmd = str(self.conf['cmd'])
        else:
            raise ValueError("file oder cmd fehlt")
        if as_bool(self.conf.get('sudo', False)):
            cmd = 'sudo -n ' + cmd
        return cmd

    def command(self):
        host = str(self.conf.get('host', '')).strip()
        if not host:
            raise ValueError("host fehlt")
        ssh = str(self.conf.get('ssh', 'ssh'))
        timeout = int(self.conf.get('connect_timeout', 5))
        return [ssh, '-o', 'BatchMode=yes', '-o', f'ConnectTimeout={timeout}', '-o', 'ServerAliveInterval=30',
                host, self.remote_cmd()]


class MqttLogSource(LogSource):
    """ type: mqtt, host, topic [port]; JSON-Nachrichten mit text/message, importance/level, category werden gelesen """

    def __init__(self, sid, conf, sink):
        super().__init__(sid, conf, sink)
        self.client = None

    def default_category(self):
        return 'mqtt-' + str(self.conf.get('host', '127.0.0.1'))

    def do_start(self):
        from nwebclient.runner.mqtt import Mqtt
        topic = str(self.conf.get('topic', '#'))
        client = Mqtt.create_client(f'nxrunner-log-{self.id}-{int(time.time())}')
        client.on_connect = lambda c, userdata, flags, rc: c.subscribe(topic) if rc == 0 else self.on_fail(rc)
        client.on_message = lambda c, userdata, msg: self.on_message(msg)
        client.connect_async(str(self.conf.get('host', '127.0.0.1')), int(self.conf.get('port', 1883)))
        client.loop_start()
        self.client = client

    def on_fail(self, rc):
        self.error = f"Verbindung fehlgeschlagen (rc {rc})"

    def on_message(self, msg):
        payload = msg.payload.decode('utf-8', errors='replace')
        try:
            data = json.loads(payload)
        except ValueError:
            data = None
        if isinstance(data, dict) and ('text' in data or 'message' in data):
            self.lines += 1
            importance = data.get('importance', data.get('level')) or self.importance
            self.sink(str(data.get('text', data.get('message'))), str(data.get('category') or self.category),
                      normalize_importance(importance, None) if importance else None)
        else:
            self.emit(f"{msg.topic}: {payload}")

    def do_stop(self):
        client, self.client = self.client, None
        if client is not None:
            client.loop_stop()
            client.disconnect()


SOURCE_TYPES = {'ssh': SshLogSource, 'cmd': CmdLogSource, 'mqtt': MqttLogSource}


class LogFoundation(r.BaseJobExecutor):
    """
      Nimmt Log-Eintraege entgegen (op=log oder aus Quellen im Arg logsources: ssh, mqtt, cmd), haelt die letzten
      logmax im Speicher und stellt sie dar. Events on_info, on_error und on_alert.
    """

    TAGS = ['system', 'log']
    ARG_KEY = 'logsources'

    def __init__(self, type='log', logmax=10000, page_size=200):
        super().__init__(type)
        self.logmax = int(logmax)
        self.page_size = int(page_size)
        self.entries = collections.deque(maxlen=self.logmax)
        self.lock = threading.Lock()
        self._sources = None
        self.define_vars('logmax', 'page_size')
        entry = {'text': 'Text', 'created': 'Unix-Zeit', 'category': 'Kategorie', 'importance': ' | '.join(IMPORTANCES)}
        self.define_event('on_info', entry, description="Eintrag mit importance info")
        self.define_event('on_error', entry, description="Eintrag mit importance error")
        self.define_event('on_alert', entry, description="Eintrag mit importance alert")
        threading.Thread(target=self.autostart_loop, daemon=True, name='log-autostart').start()

    def set_logmax(self, value):
        value = max(1, int(value))
        with self.lock:
            self.entries = collections.deque(self.entries, maxlen=value)
        self.logmax = value

    # ------------------------------------------------------------------
    # Eintraege
    # ------------------------------------------------------------------

    def add(self, text, category='default', importance=None, created=None) -> dict:
        """ Eintrag hinzufuegen; ohne importance wird sie aus dem Text erkannt """
        text = str(text)
        importance = normalize_importance(importance) if importance else guess_importance(text)
        entry = {'text': text, 'created': float(created) if created else time.time(),
                 'category': str(category or 'default'), 'importance': importance}
        with self.lock:
            self.entries.append(entry)
        if importance in ('info', 'error', 'alert'):
            self.emit_event('on_' + importance, **entry)
        return entry

    def filtered(self, category='', importance='', q='', limit=0) -> list:
        """ Eintraege (neueste zuerst), importance = Mindeststufe """
        with self.lock:
            items = list(self.entries)
        level = IMPORTANCES.index(importance) if importance in IMPORTANCES else 0
        q = q.lower()
        res = [e for e in reversed(items)
               if (not category or e['category'] == category)
               and IMPORTANCES.index(e['importance']) >= level
               and (not q or q in e['text'].lower())]
        return res[:limit] if limit else res

    def categories(self) -> list:
        with self.lock:
            return sorted({e['category'] for e in self.entries})

    # ------------------------------------------------------------------
    # Quellen
    # ------------------------------------------------------------------

    def args_value(self):
        args = getattr(self.getRoot(), 'args', None)
        if args is None or not hasattr(args, 'get'):
            args = u.Args()
        value = args.get(self.ARG_KEY, None)
        if isinstance(value, str) and value.strip()[:1] in ('[', '{'):
            value = json.loads(value)
        if isinstance(value, dict):
            value = [value] if 'type' in value else list(value.values())
        return value or []

    def sources(self) -> list:
        """ Quellen aus dem Arg logsources, einmalig erzeugt (Zustand running bleibt erhalten) """
        if self._sources is None:
            res = []
            for i, conf in enumerate(self.args_value()):
                kind = str(conf.get('type', conf.get('kind', ''))).strip().lower() if isinstance(conf, dict) else ''
                if kind not in SOURCE_TYPES:
                    self.error(f"Logquelle ungueltig: {conf!r}")
                    continue
                res.append(SOURCE_TYPES[kind](i, conf, self.add))
            self._sources = res
        return self._sources

    def source(self, sid) -> LogSource:
        for src in self.sources():
            if str(src.id) == str(sid):
                return src
        raise KeyError(f"Logquelle {sid} nicht gefunden")

    def autostart_loop(self, wait_s=60):
        """ wartet, bis der Runner eingehaengt ist (Args verfuegbar), und startet Quellen mit autostart """
        end = time.time() + wait_s
        while self.getRoot() is self and time.time() < end:
            time.sleep(1)
        for src in self.sources():
            if src.autostart:
                self.start_source(src)

    def start_source(self, src: LogSource):
        try:
            src.start()
            self.add(f"Logquelle {src.category} gestartet", 'log', 'info')
            return self.success(f"{src.category} gestartet", source=src.to_dict(), ui_reload=True)
        except Exception as e:
            self.add(f"Logquelle {src.category} nicht gestartet: {e}", 'log', 'error')
            return self.fail(f"{src.category} nicht gestartet: {e}")

    # ------------------------------------------------------------------
    # Operationen
    # ------------------------------------------------------------------

    @op("Log-Eintrag hinzufuegen (importance leer = aus dem Text erkennen)", success={'entry': 'dict'}, name='log')
    def log_op(self, text: str = '', category: str = 'default', importance: str = ''):
        if text == '':
            return self.fail("text fehlt")
        return self.success('ok', entry=self.add(text, category, importance or None))

    @op("Log-Eintraege, neueste zuerst; importance = Mindeststufe, q = Suchtext", success={'entries': 'list'},
        name='entries')
    def entries_op(self, category: str = '', importance: str = '', q: str = '', limit: int = 100):
        return self.success(entries=self.filtered(category, importance, q, limit))

    @op("Alle Log-Eintraege im Speicher loeschen", name='clear')
    def clear_op(self):
        with self.lock:
            self.entries.clear()
        return self.success("geleert", ui_reload=True)

    @op("Logquellen aus dem Arg logsources mit Status", success={'sources': 'list'}, name='sources')
    def sources_op(self):
        return self.success(sources=[s.to_dict() for s in self.sources()])

    @op("Logquelle starten", success={'source': 'dict'}, name='source_start')
    def source_start_op(self, id: str = ''):
        try:
            return self.start_source(self.source(id))
        except KeyError as e:
            return self.fail(e.args[0])

    @op("Logquelle stoppen", success={'source': 'dict'}, name='source_stop')
    def source_stop_op(self, id: str = ''):
        try:
            src = self.source(id)
        except KeyError as e:
            return self.fail(e.args[0])
        src.stop()
        self.add(f"Logquelle {src.category} gestoppt", 'log', 'info')
        return self.success(f"{src.category} gestoppt", source=src.to_dict(), ui_reload=True)

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def btn(self, title, op_name, hint, **params):
        return self.action_btn(dict(type=self.type, op=op_name, title=title, **params), hint=hint)

    def part_index(self, p: base.Page, params={}):
        self.part_sources(p)
        with p.section(h="Log"):
            p.div(self.btn("Leeren", 'clear', "Alle Einträge im Speicher löschen") + ' ' +
                  w.a("Neu laden", self.link(self.part_index), hint="Seite mit den neuesten Einträgen neu laden"))
            with self.lock:
                p.prop("Einträge", f"{len(self.entries)} / {self.logmax}")
            p.grid(self.filtered(), self.grid_cols(), params, page_size=self.page_size, search=lambda e: e['text'],
                   filters={'category': ("Kategorie", lambda e: e['category']),
                            'importance': ("Stufe", lambda e: e['importance'])},
                   count="{shown} / {total} Einträge", empty="Keine Log-Einträge")
        self.part_add(p)

    def part_add(self, p: base.Page):
        with p.section(h="Eintrag hinzufügen"):
            p.form_input('log_text', "Text", id='log_text', placeholder='Nachricht')
            p.form_input('log_category', "Kategorie", id='log_category', placeholder='default')
            p.form_input('log_importance', "Stufe", id='log_importance', placeholder='leer = erkennen',
                         suggestions=IMPORTANCES)
            p(self.action_btn_parametric("Hinzufügen", dict(type=self.type, op='log', text='#log_text',
                                                            category='#log_category', importance='#log_importance'),
                                         hint="Eintrag mit Text, Kategorie und Stufe ins Log schreiben"))

    def part_sources(self, p: base.Page):
        with p.section(h="Logquellen"):
            sources = self.sources()
            if not sources:
                p(w.alert_info(f"Keine Logquellen, im Arg '{self.ARG_KEY}' eintragen (type: ssh | mqtt | cmd)."))
                return
            col = w.Grid.col
            p.grid([s for s in sources], [
                col('category', "Kategorie", value=lambda s: s.category),
                col('type', "Typ", value=lambda s: s.kind),
                col('conf', "Konfiguration", value=lambda s: s.description()),
                col('state', "Status", html=True, value=self.col_state, sortable=False),
                col('lines', "Zeilen", value=lambda s: s.lines),
                col('actions', '', html=True, sortable=False, value=self.col_source_actions),
            ])

    def col_state(self, src: LogSource):
        if src.running:
            return f'<span title="seit {fmt_time(src.started)}">läuft</span>'
        return f'<span title="{esc(src.error)}">gestoppt</span>' + (f' ({esc(src.error)})' if src.error else '')

    def col_source_actions(self, src: LogSource):
        if src.running:
            return self.btn("Stoppen", 'source_stop', f"Logquelle {src.category} stoppen", id=src.id)
        return self.btn("Aktivieren", 'source_start', f"Logquelle {src.category} starten", id=src.id)

    def grid_cols(self):
        col = w.Grid.col
        return [
            col('created', "Zeit", value=lambda e: fmt_time(e['created'])),
            col('importance', "Stufe", html=True, value=self.col_importance,
                sort=lambda e: IMPORTANCES.index(e['importance'])),
            col('category', "Kategorie"),
            col('text', "Text", html=True, value=lambda e: '<code style="white-space:pre-wrap">' + esc(e['text']) + '</code>'),
        ]

    @staticmethod
    def col_importance(e):
        colors = {'verbose': '#6c757d', 'debug': '#6c757d', 'info': '#007bff', 'warning': '#a70',
                  'error': '#dc3545', 'alert': '#ff6b6b'}
        style = f"color:{colors[e['importance']]};font-weight:{'bold' if e['importance'] == 'alert' else 'normal'}"
        return f'<span style="{style}">{e["importance"]}</span>'
