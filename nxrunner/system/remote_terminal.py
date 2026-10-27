"""
Terminal einer anderen Instanz ueber einen Job-Speicher (Standard: MongoDB-Runner mongodb).

Befehle gehen verschluesselt (cenqueue, CryptRunner crypt noetig) mit guid und target in den Speicher, die
Ziel-Instanz fuehrt sie mit ihrem Terminal-Runner aus (MongoDB-Ticker, kurzes mongo_job_cron waehlen). Ein Thread
fragt die Ergebnisse per guid ab (cresult) und uebernimmt den Bildschirm; die Seite ist die des Terminals
(terminal.js), da RemoteTerminal dieselben Ops anbietet.

    Config (runners):
        rterm: nxrunner.system.remote_terminal:RemoteTerminal(target="pi4")
        rterm: nxrunner.system.remote_terminal:RemoteTerminal(target="pi4", store="mongodb", remote_type="terminal")

    Jobs:
        {"type": "rterm", "op": "input", "text": "ls -l"}     Befehl senden (remote op=exec)
        {"type": "rterm", "op": "key", "key": "ctrl_c"}       Taste senden, danach Bildschirm holen
        {"type": "rterm", "op": "refresh"}                    Bildschirm holen (remote op=get_terminal)
        {"type": "rterm", "op": "screen", "version": 12}      lokaler Stand, nur bei Aenderung

    Job-Speicher: Runner mit op=cenqueue (jobdef, guid, target) und op=cresult (guid, delete), siehe MongoDB.
"""
import threading
import time

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import dev as d
from nwebclient import util as u

from nxrunner.system.terminal import Terminal, terminal_page


class RemoteTerminal(r.BaseJobExecutor):

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
        self.target = target
        self.store = store
        self.remote_type = remote_type
        self.wait_time = float(wait_time)
        self.poll_interval = float(poll_interval)
        self.timeout = float(timeout)
        self.define_vars('target', 'store', 'remote_type')
        self.define_sig(d.PStr('op', 'screen'), d.PInt('version', -1)).r(
            success='bool', lines='list', cursor='dict', version='int', running='bool', unchanged='bool')
        self.define_sig(d.PStr('op', 'input'), d.PStr('text', ''), d.PInt('enter', 1)).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'key'), d.PStr('key', 'ctrl_c')).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'refresh')).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'restart')).r(success='bool', message='str')
        self.lock = threading.Lock()
        self.lines = []
        self.cursor = None
        self.running = True
        self.version = 0
        self.error = ''
        self.seq = 0            # laufende Nummer gesendeter Jobs
        self.applied_seq = 0    # Nummer des Jobs, dessen Bildschirm angezeigt wird
        self.pending = {}       # guid -> {seq, ts, screen}
        self.poller = None
        self.last_update = None
        self.started = False    # Bildschirm der Ziel-Instanz schon angefordert

    # --- Job-Speicher ---------------------------------------------------------------------------

    def store_execute(self, data: dict) -> dict:
        return self.getRoot().jobexecutor.execute({'type': self.store, **data})

    def send(self, job: dict, screen: bool) -> str:
        """
          Job verschluesselt in den Speicher schreiben.
          :param screen: Ergebnis enthaelt den Bildschirm (exec, get_terminal)
          :return: guid
        """
        if not self.target:
            raise RuntimeError('target (Name der Ziel-Instanz) ist nicht gesetzt')
        guid = u.guid()
        res = self.store_execute(dict(op='cenqueue', jobdef={'type': self.remote_type, **job}, guid=guid,
                                      target=self.target))
        if not isinstance(res, dict) or not res.get('success', False):
            message = res.get('message', res) if isinstance(res, dict) else res
            raise RuntimeError(f'Job-Speicher {self.store}: {message}')
        with self.lock:
            self.seq += 1
            self.pending[guid] = dict(seq=self.seq, ts=time.time(), screen=screen)
            self.error = ''
            self.version += 1
            if self.poller is None:
                self.poller = threading.Thread(target=self.poll_loop, daemon=True, name='remote-terminal-' + self.type)
                self.poller.start()
        return guid

    def poll_loop(self):
        while True:
            time.sleep(self.poll_interval)
            with self.lock:
                guids = list(self.pending.keys())
                if not guids:
                    self.poller = None
                    return
            for guid in guids:
                try:
                    self.poll(guid)
                except Exception as e:
                    self.set_error(f'Abfrage {guid}: {type(e).__name__}: {e}')

    def poll(self, guid):
        res = self.store_execute(dict(op='cresult', guid=guid, delete=1))
        with self.lock:
            job = self.pending.get(guid)
            if job is None:
                return
            if not isinstance(res, dict) or not res.get('success', False):
                self.error = str(res.get('message', res) if isinstance(res, dict) else res)
                self.version += 1
            elif res.get('pending', False):
                if time.time() - job['ts'] > self.timeout:
                    del self.pending[guid]
                    self.error = f'Keine Antwort von {self.target} nach {int(self.timeout)} s'
                    self.version += 1
            else:
                del self.pending[guid]
                self.apply(job, res.get('result') or {})
                self.version += 1

    def apply(self, job: dict, result: dict):
        """ Ergebnis uebernehmen (unter self.lock), aeltere Bildschirme ueberschreiben keine neueren """
        self.last_update = time.time()
        if not result.get('success', False):
            self.error = str(result.get('message', 'Fehler auf ' + self.target))
            return
        if job['screen'] and 'value' in result and job['seq'] > self.applied_seq:
            self.applied_seq = job['seq']
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

    def set_error(self, message):
        with self.lock:
            self.error = message
            self.version += 1

    # --- Jobs -----------------------------------------------------------------------------------

    def screen_dict(self):
        with self.lock:
            status = self.target or '(kein target)'
            if self.pending:
                status += f' – wartet auf {len(self.pending)} Antwort(en)'
            elif self.last_update is not None:
                status += ' – Stand ' + time.strftime('%H:%M:%S', time.localtime(self.last_update))
            if self.error:
                status += ' – ' + self.error
            return dict(lines=list(self.lines), cursor=self.cursor, version=self.version, running=self.running,
                        exit_code=None, mode='remote ' + self.store, title=status, pending=len(self.pending))

    def execute_screen(self, data):
        if not self.started and self.target:
            # erster Aufruf: Bildschirm der Ziel-Instanz holen
            self.started = True
            self.execute_refresh({})
        if str(data.get('version', '')) == str(self.version):
            return self.success('ok', unchanged=True, version=self.version)
        return self.success('ok', **self.screen_dict())

    def run_send(self, *jobs):
        """ jobs: (job, screen) in dieser Reihenfolge senden """
        try:
            for job, screen in jobs:
                self.send(job, screen)
        except Exception as e:
            self.set_error(str(e))
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
        error = '' if self.target else 'target (Name der Ziel-Instanz) ist nicht gesetzt'
        terminal_page(p, self.type, error, title='Remote-Terminal ' + self.target,
                      buttons='<button type="button" id="term_refresh">Aktualisieren</button>')
        p.div(f'Job-Speicher: {self.store}, Terminal auf der Ziel-Instanz: {self.remote_type}. '
              'Die Ziel-Instanz arbeitet die Jobs im Takt ihres Tickers ab (mongo_job_cron).')
