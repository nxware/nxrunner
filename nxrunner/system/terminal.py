"""
Terminal (VT100) im Runner.

Startet eine Shell in einem Pseudo-Terminal (Linux: pty, Windows: pywinpty, sonst Pipes) und wertet die Ausgabe mit
pyte als VT100-/xterm-Terminal aus. part_index zeigt den Bildschirm an und fragt ihn regelmaessig ab, unter dem
Terminal ist ein Eingabefeld fuer Textbefehle.

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
        import fcntl
        import pty
        import struct
        import termios
        pid, fd = pty.fork()
        if pid == 0:
            try:
                if self.cwd:
                    os.chdir(self.cwd)
                os.execvpe(args[0], args, env)
            finally:
                os._exit(127)
        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack('HHHH', self.rows, self.cols, 0, 0))
        self.mode, self.proc, self.fd = 'pty', pid, fd
        self.decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')

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
                self.version += 1
        self.exit_code = self.wait()
        with self.lock:
            self.running = False
            self.version += 1

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

    # --- Seite ----------------------------------------------------------------------------------

    def part_index(self, p: b.Page, params={}):
        if not self.running and self.screen is None:
            self.start()
        error = 'Shell konnte nicht gestartet werden: ' + self.error if self.error else ''
        terminal_page(p, self.type, error)


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
