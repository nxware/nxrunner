"""
Terminal (VT100) im Runner.

Startet eine Shell in einem Pseudo-Terminal (Linux: pty, Windows: pywinpty, sonst Pipes) und wertet die Ausgabe mit
pyte als VT100-/xterm-Terminal aus. part_index zeigt den Bildschirm an und fragt ihn regelmaessig ab, unter dem
Terminal ist ein Eingabefeld fuer Textbefehle.
page_vt100 (?type=terminal&terminal=vt100) ist eine Vollbild-Terminalemulation mit xterm.js, die die Rohausgabe
(Op output) im Browser auswertet und jede Taste direkt sendet, z.B. fuer einen iframe (CodeServer).

    Config (runners):
        terminal: nxrunner.system.terminal:Terminal()
        terminal: nxrunner.system.terminal:Terminal(shell="/bin/bash", cols=120, rows=32)

    Jobs:
        {"type": "terminal", "op": "input", "text": "ls -l"}            Befehl senden (mit Enter)
        {"type": "terminal", "op": "input", "text": "q", "enter": 0}    Text ohne Enter senden
        {"type": "terminal", "op": "key", "key": "ctrl_c"}             Sondertaste, siehe KEYS
        {"type": "terminal", "op": "screen"}                           Bildschirminhalt
        {"type": "terminal", "op": "screen", "version": 12}            nur bei Aenderung, sonst unchanged=True
        {"type": "terminal", "op": "restart"}
        {"type": "terminal", "op": "get_terminal", "wait_time": 0}     Bildschirm als Text (value) mit cursor
        {"type": "terminal", "op": "exec", "cmd": "ls", "wait_time": 1}   Befehl senden, warten, Bildschirm als Text mit cursor
        {"type": "terminal", "op": "output", "pos": 0, "session": 1, "wait": 2}  Rohausgabe ab pos (Long-Poll)
        {"type": "terminal", "op": "resize", "cols": 120, "rows": 40}
"""
import codecs
import json
import os
import shlex
import subprocess
import threading
import time
from importlib import resources

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import dev as d
from nwebclient.runner import op


def resource(name) -> str:
    return resources.files('nxrunner').joinpath('resources').joinpath(name).read_text(encoding='utf-8')


class Terminal(r.BaseJobExecutor):

    TAGS = ['system', 'terminal']

    MODULES = ['pyte']

    KEYS = {
        'enter': '\r', 'tab': '\t', 'esc': '\x1b', 'backspace': '\x7f',
        'up': '\x1b[A', 'down': '\x1b[B', 'right': '\x1b[C', 'left': '\x1b[D',
        'home': '\x1b[H', 'end': '\x1b[F', 'pgup': '\x1b[5~', 'pgdown': '\x1b[6~',
        'ctrl_c': '\x03', 'ctrl_d': '\x04', 'ctrl_z': '\x1a', 'ctrl_l': '\x0c',
    }

    RAW_LIMIT = 256 * 1024  # Zeichen Rohausgabe, die fuer die VT100-Ansicht vorgehalten werden
    XTERM_CDN = 'https://cdn.jsdelivr.net/npm/'
    XTERM_FILES = ['@xterm/xterm@5.5.0/lib/xterm.min.js', '@xterm/addon-fit@0.10.0/lib/addon-fit.min.js']
    XTERM_CSS = '@xterm/xterm@5.5.0/css/xterm.min.css'

    def __init__(self, type='terminal', shell=None, cols=100, rows=30, cwd=None):
        """
        :param shell: Befehl der Shell, Standard $SHELL bzw. /bin/bash, unter Windows %COMSPEC%
        :param cwd: Arbeitsverzeichnis der Shell
        """
        super().__init__(type)
        self.shell = shell or self.default_shell()
        self.cols = int(cols)
        self.rows = int(rows)
        self.cwd = cwd
        self.define_vars('shell', 'cols', 'rows')
        self.define_sig(d.PStr('op', 'screen'), d.PInt('version', -1)).r(
            success='bool', lines='list', cursor='dict', version='int', running='bool', unchanged='bool')
        self.define_sig(d.PStr('op', 'input'), d.PStr('text', ''), d.PInt('enter', 1)).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'key'), d.PStr('key', 'ctrl_c')).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'restart')).r(success='bool', message='str')
        self.lock = threading.Lock()
        self.changed = threading.Condition(self.lock)  # signalisiert neue Ausgabe (Long-Poll von output)
        self.raw_text = ''    # Rohausgabe fuer die VT100-Ansicht
        self.raw_start = 0    # absolute Position von raw_text[0]
        self.session = 0      # wird bei jedem Start erhoeht, die VT100-Ansicht setzt sich dann zurueck
        self.screen = None
        self.stream = None
        self.version = 0
        self.mode = None      # pty, winpty, pipe
        self.running = False
        self.exit_code = None
        self.error = ''
        self.proc = None      # pid (pty), PtyProcess (winpty) oder Popen (pipe)
        self.fd = None

    @staticmethod
    def default_shell():
        if os.name == 'nt':
            return os.environ.get('COMSPEC', 'cmd.exe')
        return os.environ.get('SHELL', '/bin/bash')

    # --- Prozess --------------------------------------------------------------------------------

    def start(self):
        import pyte
        self.screen = pyte.Screen(self.cols, self.rows)
        self.stream = pyte.Stream(self.screen)
        self.exit_code = None
        self.error = ''
        with self.lock:
            self.raw_start += len(self.raw_text)
            self.raw_text = ''
            self.session += 1
        args = shlex.split(self.shell, posix=os.name != 'nt')
        env = dict(os.environ, TERM='xterm', COLUMNS=str(self.cols), LINES=str(self.rows))
        try:
            if os.name != 'nt':
                self.start_pty(args, env)
            else:
                try:
                    self.start_winpty(args)
                except ImportError:
                    self.start_pipe(args, env)
        except OSError as e:
            self.error = str(e)
            self.running = False
            return
        self.running = True
        threading.Thread(target=self.read_loop, daemon=True, name='terminal-' + self.type).start()

    def start_pty(self, args, env):
        import pty
        pid, fd = pty.fork()
        if pid == 0:
            try:
                if self.cwd:
                    os.chdir(self.cwd)
                os.execvpe(args[0], args, env)
            finally:
                os._exit(127)
        self.mode, self.proc, self.fd = 'pty', pid, fd
        self.set_winsize(self.cols, self.rows)
        self.decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')

    def set_winsize(self, cols: int, rows: int):
        """ Groesse des Pseudo-Terminals setzen (pty/winpty), bei Pipes ohne Wirkung """
        if self.mode == 'pty':
            import fcntl
            import struct
            import termios
            fcntl.ioctl(self.fd, termios.TIOCSWINSZ, struct.pack('HHHH', rows, cols, 0, 0))
        elif self.mode == 'winpty':
            self.proc.setwinsize(rows, cols)

    def start_winpty(self, args):
        from winpty import PtyProcess
        self.proc = PtyProcess.spawn(args, cwd=self.cwd, dimensions=(self.rows, self.cols))
        self.mode = 'winpty'

    def start_pipe(self, args, env):
        # ohne Pseudo-Terminal: keine Vollbildprogramme, aber einfache Befehle funktionieren
        import pyte
        self.proc = subprocess.Popen(args, cwd=self.cwd, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, bufsize=0)
        self.mode = 'pipe'
        self.screen.set_mode(pyte.modes.LNM)  # \n wie \r\n behandeln
        self.decoder = codecs.getincrementaldecoder('oem' if os.name == 'nt' else 'utf-8')(errors='replace')

    def read(self):
        """ liest blockierend die naechste Ausgabe, '' bei Ende """
        if self.mode == 'pty':
            try:
                data = os.read(self.fd, 4096)
            except OSError:  # EIO, wenn die Shell beendet ist
                data = b''
            return self.decoder.decode(data) if data else ''
        if self.mode == 'winpty':
            try:
                return self.proc.read(4096)
            except EOFError:
                return ''
        data = self.proc.stdout.read1(4096) if hasattr(self.proc.stdout, 'read1') else self.proc.stdout.read(1)
        return self.decoder.decode(data) if data else ''

    def read_loop(self):
        while True:
            text = self.read()
            if text == '':
                break
            with self.lock:
                self.stream.feed(text)
                self.append_raw(text)
                self.version += 1
                self.changed.notify_all()
        self.exit_code = self.wait()
        with self.lock:
            self.running = False
            self.version += 1
            self.changed.notify_all()

    def append_raw(self, text: str):
        """ Rohausgabe anhaengen und auf RAW_LIMIT kuerzen (Aufruf mit self.lock) """
        self.raw_text += text
        cut = len(self.raw_text) - self.RAW_LIMIT
        if cut > 0:
            self.raw_text = self.raw_text[cut:]
            self.raw_start += cut

    def wait(self):
        try:
            if self.mode == 'pty':
                os.close(self.fd)
                return os.waitstatus_to_exitcode(os.waitpid(self.proc, 0)[1])
            return self.proc.wait()
        except (OSError, ChildProcessError):
            return None

    def write(self, text: str):
        if not self.running:
            self.start()
            if not self.running:
                raise RuntimeError('Shell konnte nicht gestartet werden: ' + self.error)
        if self.mode == 'pty':
            os.write(self.fd, text.encode('utf-8'))
        elif self.mode == 'winpty':
            self.proc.write(text)
        else:
            enc = 'oem' if os.name == 'nt' else 'utf-8'
            self.proc.stdin.write(text.replace('\r', os.linesep).encode(enc, errors='replace'))
            self.proc.stdin.flush()

    def stop(self):
        if not self.running:
            return
        try:
            if self.mode == 'pty':
                import signal
                os.kill(self.proc, signal.SIGHUP)
            elif self.mode == 'winpty':
                self.proc.terminate(force=True)
            else:
                self.proc.kill()
        except OSError:
            pass

    # --- Jobs -----------------------------------------------------------------------------------

    def screen_dict(self):
        with self.lock:
            if self.screen is None:
                return dict(lines=[], cursor=None, version=self.version, running=False, mode=self.mode)
            c = self.screen.cursor
            return dict(lines=list(self.screen.display), cursor=dict(x=c.x, y=c.y, hidden=c.hidden),
                        version=self.version, running=self.running, exit_code=self.exit_code, mode=self.mode,
                        title=self.screen.title)

    def execute_screen(self, data):
        if not self.running and self.screen is None:
            self.start()
        if str(data.get('version', '')) == str(self.version):
            return self.success('ok', unchanged=True, version=self.version)
        return self.success('ok', **self.screen_dict())

    def execute_input(self, data):
        text = data.get('text', '')
        if str(data.get('enter', '1')) not in ('0', 'false', 'False'):
            text += '\r'
        try:
            self.write(text)
        except (OSError, RuntimeError) as e:
            return self.fail(str(e))
        return self.success('ok')

    def execute_key(self, data):
        key = data.get('key', '')
        if key not in self.KEYS:
            return self.fail('Unbekannte Taste: ' + key, keys=list(self.KEYS.keys()))
        return self.execute_input(dict(text=self.KEYS[key], enter=0))

    def execute_restart(self, data):
        self.stop()
        self.start()
        if not self.running:
            return self.fail('Shell konnte nicht gestartet werden: ' + self.error)
        return self.success('Shell gestartet: ' + self.shell + ' (' + self.mode + ')')

    def screen_text(self, screen=None):
        screen = screen or self.screen_dict()
        return '\n'.join(line.rstrip() for line in screen['lines']).rstrip('\n')

    def screen_result(self):
        """ Bildschirm als Text mit Cursor (Ergebnis von exec und get_terminal) """
        screen = self.screen_dict()
        return self.success('ok', value=self.screen_text(screen), cursor=screen['cursor'], running=self.running)

    @op("Bildschirminhalt des Terminals als Text, optional nach wait_time Sekunden",
        result={'success': 'bool', 'message': 'str', 'value': 'str', 'cursor': 'dict'})
    def get_terminal(self, wait_time: float = 0):
        if not self.running and self.screen is None:
            self.start()
        if float(wait_time) > 0:
            time.sleep(float(wait_time))
        return self.screen_result()

    # exec ist eine eingebaute Funktion, daher abweichender Methodenname
    @op("Schreibt einen Befehl ins Terminal, wartet wait_time Sekunden und liefert den Bildschirm",
        result={'success': 'bool', 'message': 'str', 'value': 'str', 'cursor': 'dict'}, name='exec')
    def exec_command(self, cmd: str = None, wait_time: float = 1):
        # cmd=None: kein Button auf der buttons-Seite (is_direct_callable), da ein Befehl erwartet wird
        if cmd is None:
            return self.fail('cmd fehlt')
        try:
            self.write(str(cmd) + '\r')
        except (OSError, RuntimeError) as e:
            return self.fail(str(e))
        time.sleep(float(wait_time))
        return self.screen_result()

    def raw_since(self, pos: int, session: int) -> dict:
        """ Rohausgabe ab pos, reset=True wenn der Client neu aufbauen muss (Neustart, Puffer gekuerzt) """
        end = self.raw_start + len(self.raw_text)
        reset = session != self.session or pos < self.raw_start or pos > end
        data = self.raw_text if reset else self.raw_text[pos - self.raw_start:]
        return dict(data=data, pos=end, reset=reset, session=self.session, running=self.running,
                    exit_code=self.exit_code, mode=self.mode)

    @op("Rohausgabe des Terminals ab Position pos (fuer Terminalemulation im Browser), wartet bis zu wait "
        "Sekunden auf neue Ausgabe. reset=True: Client muss das Terminal leeren und data komplett ausgeben",
        result={'success': 'bool', 'data': 'str', 'pos': 'int', 'reset': 'bool', 'session': 'int',
                'running': 'bool'})
    def output(self, pos: int = -1, session: int = -1, wait: float = 0):
        if not self.running and self.screen is None:
            self.start()
        pos, session, wait = int(pos), int(session), min(max(float(wait), 0), 10)
        with self.changed:
            if wait > 0 and self.running and session == self.session:
                self.changed.wait_for(lambda: self.raw_start + len(self.raw_text) != pos or not self.running,
                                      timeout=wait)
            res = self.raw_since(pos, session)
        if self.error:
            res['error'] = 'Shell konnte nicht gestartet werden: ' + self.error
        return self.success('ok', **res)

    @op("Groesse des Terminals aendern (Spalten und Zeilen), gilt fuer alle Ansichten",
        result={'success': 'bool', 'cols': 'int', 'rows': 'int'})
    def resize(self, cols: int = 100, rows: int = 30):
        cols, rows = min(max(int(cols), 10), 500), min(max(int(rows), 2), 200)
        with self.lock:
            self.cols, self.rows = cols, rows
            if self.screen is not None:
                self.screen.resize(rows, cols)
        if self.running:
            try:
                self.set_winsize(cols, rows)
            except OSError as e:
                return self.fail(str(e))
        return self.success('ok', cols=cols, rows=rows)

    # --- Seite ----------------------------------------------------------------------------------

    def part_index(self, p: b.Page, params={}):
        if not self.running and self.screen is None:
            self.start()
        error = 'Shell konnte nicht gestartet werden: ' + self.error if self.error else ''
        vt100 = (f'<a href="{self.dispatcher_link(self.page_vt100)}" target="_blank" rel="noopener" '
                 'title="Vollbild-Terminalemulation (xterm.js) in neuem Fenster">VT100-Ansicht</a>')
        terminal_page(p, self.type, error, buttons=vt100)

    def page_vt100(self, params={}):
        """ Vollbild-Terminalemulation (xterm.js) ohne Navigation, z.B. fuer einen iframe """
        p = b.Page(owner=self)
        p['title'] = 'Terminal ' + self.type
        p.style(self.XTERM_CDN + self.XTERM_CSS)
        p.style('html, body { height: 100%; background: #1e1e1e; overflow: hidden; } '
                '#vt100 { height: 100%; box-sizing: border-box; padding: 4px; }')
        p('<div id="vt100"></div>')
        for js in self.XTERM_FILES:
            p.script(self.XTERM_CDN + js)
        p.script(resource('vt100.js').replace('__TYPE__', json.dumps(self.type)))
        return p.render('blank_page')


def terminal_page(p: b.Page, runner_type: str, error='', title='Terminal', buttons=''):
    """
      Terminal-Oberflaeche (Bildschirm, Eingabe, Tasten) fuer einen Runner mit den Ops screen, input, key, restart
      :param buttons: weitere Buttons hinter "Neu starten" (z.B. term_refresh)
    """
    with p.section(h=title):
        if error:
            p.alert_error(error)
        p.div('', id='term_status')
        p('<pre id="term_screen" style="background: #111; color: #ddd; padding: 6px; margin: 0; line-height: 1.2; '
          'font-family: monospace; overflow-x: auto; white-space: pre"></pre>')
        p('<div style="display: flex; gap: 4px; margin-top: 4px">'
          '<input id="term_input" style="flex: 1; font-family: monospace" autocomplete="off" '
          'placeholder="Befehl, Enter sendet; Pfeil hoch/runter: Verlauf">'
          '<button type="button" id="term_send">Senden</button></div>')
        keys = ''.join(f'<button type="button" class="term_key" data-key="{k}">{t}</button> '
                       for k, t in [('ctrl_c', 'Strg+C'), ('ctrl_d', 'Strg+D'), ('tab', 'Tab'), ('esc', 'Esc'),
                                    ('up', '&uarr;'), ('down', '&darr;'), ('enter', 'Enter')])
        p.div(keys + '<button type="button" id="term_restart">Neu starten</button> ' + buttons, style='margin-top: 4px')
    p.script(resource('terminal.js').replace('__TYPE__', json.dumps(runner_type)))
