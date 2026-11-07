"""
Terminal einer anderen Instanz ueber einen Job-Speicher (Standard: MongoDB-Runner mongodb).

Befehle gehen verschluesselt (cenqueue, CryptRunner crypt noetig) mit guid und target in den Speicher, die
Ziel-Instanz fuehrt sie mit ihrem Terminal-Runner aus (MongoDB-Ticker, kurzes mongo_job_cron waehlen). Ein Thread
fragt die Ergebnisse per guid ab (cresult) und uebernimmt den Bildschirm; die Seite ist die des Terminals
(terminal.js), da RemoteTerminal dieselben Ops anbietet. Senden und Abfragen uebernimmt
nxrunner.jobstore:RemoteJobMixin.

    Config (runners):
        rterm: nxrunner.system.remote_terminal:RemoteTerminal(target="pi4")
        rterm: nxrunner.system.remote_terminal:RemoteTerminal(target="pi4", store="mongodb", remote_type="terminal")

    Jobs:
        {"type": "rterm", "op": "input", "text": "ls -l"}     Befehl senden (remote op=exec)
        {"type": "rterm", "op": "key", "key": "ctrl_c"}       Taste senden, danach Bildschirm holen
        {"type": "rterm", "op": "refresh"}                    Bildschirm holen (remote op=get_terminal)
        {"type": "rterm", "op": "screen", "version": 12}      lokaler Stand, nur bei Aenderung

    Job-Speicher: Runner mit nxrunner.jobstore:JobStoreMixin (op=cenqueue, op=cresult), z.B. MongoDB.
"""
from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import dev as d

from nxrunner.jobstore import RemoteJobMixin
from nxrunner.system.terminal import Terminal, terminal_page


class RemoteTerminal(r.BaseJobExecutor, RemoteJobMixin):

    TAGS = ['system', 'terminal', 'remote']

    def __init__(self, type='remote_terminal', target='', store='mongodb', remote_type='terminal', wait_time=1,
                 poll_interval=10, timeout=6000):
        """
        :param target: Name der Ziel-Instanz (nwebclient.nx.get_name() dort, i.d.R. der Hostname)
        :param store: type des Job-Speicher-Runners
        :param remote_type: type des Terminal-Runners auf der Ziel-Instanz
        :param wait_time: Sekunden, die die Ziel-Instanz nach einer Eingabe auf Ausgabe wartet
        :param poll_interval: Sekunden zwischen den Abfragen der Ergebnisse
        :param timeout: Sekunden, nach denen eine Antwort verworfen wird
        """
        super().__init__(type)
        self.init_remote(target, store, remote_type, poll_interval, timeout)
        self.wait_time = float(wait_time)
        self.define_vars('target', 'store', 'remote_type')
        self.define_sig(d.PStr('op', 'screen'), d.PInt('version', -1)).r(
            success='bool', lines='list', cursor='dict', version='int', running='bool', unchanged='bool')
        self.define_sig(d.PStr('op', 'input'), d.PStr('text', ''), d.PInt('enter', 1)).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'key'), d.PStr('key', 'ctrl_c')).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'refresh')).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'restart')).r(success='bool', message='str')
        self.lines = []
        self.cursor = None
        self.running = True
        self.version = 0
        self.applied_seq = 0    # Nummer des Jobs, dessen Bildschirm angezeigt wird
        self.started = False    # Bildschirm der Ziel-Instanz schon angefordert

    # --- RemoteJobMixin -------------------------------------------------------------------------

    def remote_job_label(self, job: dict):
        """ Anzeige des Jobs in der Statuszeile, None fuer get_terminal nach einer Eingabe """
        op = job.get('op')
        if op == 'exec':
            return str(job.get('cmd', ''))
        if op == 'input':
            return repr(job.get('text', '')) + ' (ohne Enter)'
        if op == 'key':
            return 'Taste ' + str(job.get('key', ''))
        if op == 'get_terminal':
            return None if 'wait_time' in job else 'Bildschirm abrufen'
        return str(op)

    def remote_changed(self):
        self.version += 1

    def on_remote_result(self, meta: dict, result: dict):
        """ Bildschirm uebernehmen, aeltere Bildschirme ueberschreiben keine neueren """
        if not result.get('success', False) or not meta['screen'] or 'value' not in result:
            return
        if meta['seq'] > self.applied_seq:
            self.applied_seq = meta['seq']
            self.lines = str(result['value']).split('\n')
            self.cursor = result.get('cursor')
            self.running = bool(result.get('running', True))
            self.pad_cursor_line()

    def pad_cursor_line(self):
        """ exec liefert Zeilen ohne Leerzeichen am Ende, Cursor-Zeile wieder auffuellen """
        c = self.cursor
        if not isinstance(c, dict) or 'x' not in c or 'y' not in c:
            return
        while len(self.lines) <= c['y']:
            self.lines.append('')
        self.lines[c['y']] = self.lines[c['y']].ljust(c['x'] + 1)

    # --- Jobs -----------------------------------------------------------------------------------

    def screen_dict(self):
        with self.remote_lock:
            return dict(lines=list(self.lines), cursor=self.cursor, version=self.version, running=self.running,
                        exit_code=None, mode='remote ' + self.store, title=self.remote_status(),
                        pending=len(self.remote_pending))

    def execute_screen(self, data):
        if not self.started and self.target:
            # erster Aufruf: Bildschirm der Ziel-Instanz holen
            self.started = True
            self.execute_refresh({})
        if str(data.get('version', '')) == str(self.version):
            return self.success('ok', unchanged=True, version=self.version)
        return self.success('ok', **self.screen_dict())

    def run_send(self, *jobs):
        """ jobs: (job, screen) in dieser Reihenfolge senden, screen: Ergebnis enthaelt den Bildschirm """
        try:
            for job, screen in jobs:
                self.remote_send(job, screen=screen)
        except Exception as e:
            self.set_remote_error(str(e))
            return self.fail(str(e))
        return self.success('gesendet, wartet auf Antwort')

    def execute_input(self, data):
        text = data.get('text', '')
        if str(data.get('enter', '1')) not in ('0', 'false', 'False'):
            return self.run_send((dict(op='exec', cmd=text, wait_time=self.wait_time), True))
        return self.run_send((dict(op='input', text=text, enter=0), False),
                             (dict(op='get_terminal', wait_time=self.wait_time), True))

    def execute_key(self, data):
        key = data.get('key', '')
        if key not in Terminal.KEYS:
            return self.fail('Unbekannte Taste: ' + key, keys=list(Terminal.KEYS.keys()))
        return self.run_send((dict(op='key', key=key), False),
                             (dict(op='get_terminal', wait_time=self.wait_time), True))

    def execute_refresh(self, data):
        return self.run_send((dict(op='get_terminal'), True))

    def execute_restart(self, data):
        return self.run_send((dict(op='restart'), False), (dict(op='get_terminal', wait_time=self.wait_time), True))

    # --- Seite ----------------------------------------------------------------------------------

    def part_index(self, p: b.Page, params={}):
        error = self.check_store() if self.target else 'target (Name der Ziel-Instanz) ist nicht gesetzt'
        terminal_page(p, self.type, error, title='Remote-Terminal ' + self.target,
                      buttons='<button type="button" id="term_refresh" title="Bildschirm der Ziel-Instanz neu abrufen">Aktualisieren</button>')
        p.div(f'Job-Speicher: {self.store}, Terminal auf der Ziel-Instanz: {self.remote_type}. '
              'Die Ziel-Instanz arbeitet die Jobs im Takt ihres Tickers ab (mongo_job_cron).')
