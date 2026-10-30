"""
Runner in einem Docker-Container ausfuehren, z.B. wenn ihm lokal Module fehlen ("In Docker laden" in der Registry).

Ablauf beim Start (im Hintergrund, Fortschritt auf der Seite des Runners):
  1. Image nxrunner-<entry point> bauen, falls es fehlt: python:3.12-slim + pip install der angegebenen Pakete
     (Vorschlag: nwebclient, Paket des Runners, fehlende MODULES) und ein Startskript
  2. Container starten: Port 7070 des Containers auf einen freien Port von 127.0.0.1 weitergeleitet,
     zusaetzliche Ports und Mounts wie angegeben, Konstruktor-Parameter per Umgebungsvariable NX_RUNNER_ARGS
  3. Alle Jobs (auch die Seiten der UI) per HTTP an den Container weiterleiten

Die Container laufen nur, solange nxrunner laeuft (beim Beenden werden sie gestoppt und entfernt).

Jobs des Proxys selbst (alle anderen gehen an den Runner im Container):
    op=docker_status | docker_start | docker_stop | docker_restart | docker_rebuild | docker_logs
"""
import atexit
import html
import importlib.util
import io
import json
import os
import re
import socket
import subprocess
import tarfile
import threading
import time
import weakref

import requests

from nwebclient import runner as r
from nwebclient import dev as d
from nwebclient import base as b
from nwebclient import web as w

CONTAINER_PORT = 7070
BASE_IMAGE = 'python:3.12-slim'

# startet den Runner im Container mit Konstruktor-Parametern aus NX_RUNNER_ARGS
START_SCRIPT = '''import json, os
from nwebclient import runner as r, util as u
cls = u.load_class(os.environ["NX_RUNNER_SPEC"])
runner = cls(**json.loads(os.environ.get("NX_RUNNER_ARGS") or "{}"))
r.run(u.Args(["nxrunner-docker", "--rest", "--port", "%d"], read_nxd=False), runner)
''' % CONTAINER_PORT


def parse_lines(value, comma=True):
    """ Textfeld/Liste -> Liste nicht leerer Zeilen (comma: auch Komma trennt) """
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    text = str(value or '')
    return [line.strip() for line in (text.replace(',', '\n') if comma else text).splitlines() if line.strip()]


def parse_mounts(lines) -> dict:
    """ ['/host:/container', '/host2:/data:ro'] -> {'/host': {'bind': '/container', 'mode': 'rw'}, ...} """
    res = {}
    for line in lines:
        parts = line.split(':')
        if len(parts) not in (2, 3) or not parts[0] or not parts[1]:
            raise ValueError(f'Mount ungueltig (Host:Container[:ro]): {line}')
        mode = parts[2] if len(parts) == 3 else 'rw'
        if mode not in ('ro', 'rw'):
            raise ValueError(f'Mount-Modus ro oder rw erwartet: {line}')
        res[parts[0]] = {'bind': parts[1], 'mode': mode}
    return res


def parse_ports(lines) -> dict:
    """ ['8080:80', '5000'] -> {'80/tcp': ('127.0.0.1', 8080), '5000/tcp': ('127.0.0.1', 5000)} """
    res = {}
    for line in lines:
        host, _, container = line.rpartition(':')
        if not container.isdigit() or (host and not host.isdigit()):
            raise ValueError(f'Port ungueltig (Host:Container oder Port): {line}')
        res[f'{container}/tcp'] = ('127.0.0.1', int(host or container))
    return res


def requirement_name(requirement):
    """ 'nxml @ git+https://...' / 'scipy>=1' -> 'nxml' / 'scipy' """
    m = re.match(r'\s*([A-Za-z0-9_.\-]+)', requirement)
    return m.group(1) if m else ''


def local_source(name):
    """
      Quellverzeichnis (mit setup.py/pyproject.toml), wenn das Paket hier aus einem Quellbaum statt aus
      site-packages geladen wird (lokale Entwicklung oder pip install -e), sonst None
    """
    try:
        spec = importlib.util.find_spec(name.replace('-', '_'))
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.origin or spec.origin in ('built-in', 'frozen'):
        return None
    package_dir = os.path.dirname(os.path.abspath(spec.origin))
    if any(part in ('site-packages', 'dist-packages') for part in package_dir.split(os.sep)):
        return None
    root = os.path.dirname(package_dir)
    if any(os.path.isfile(os.path.join(root, f)) for f in ('setup.py', 'pyproject.toml')):
        return root
    return None


def source_files(root):
    """ Dateien eines Quellbaums: bei git nur versionierte (keine lokalen Konfigurationen wie nweb.ini) """
    try:
        out = subprocess.run(['git', 'ls-files', '-z'], cwd=root, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, check=True, timeout=30).stdout.decode()
        return [f for f in out.split('\0') if f and os.path.isfile(os.path.join(root, f))]
    except (OSError, subprocess.SubprocessError):
        skip = {'.git', '__pycache__', 'build', 'dist', '.venv', 'venv', '.idea', 'node_modules'}
        files = []
        for path, dirs, names in os.walk(root):
            dirs[:] = [d for d in dirs if d not in skip and not d.endswith('.egg-info')]
            files += [os.path.relpath(os.path.join(path, n), root) for n in names]
        return files


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class DockerProxy(r.BaseJobExecutor):

    MODULES = ['docker']

    # alle laufenden Proxys, damit ihre Container beim Beenden von nxrunner gestoppt werden
    instances = weakref.WeakSet()

    def __init__(self, type, entry, spec, packages, mounts=(), ports=(), ctor_args=None, start=True):
        """
        :param entry: Name des Entry-Points (Name des Images nxrunner-<entry>)
        :param spec: modul:Klasse des Runners
        :param packages: pip-Pakete fuer das Image
        :param mounts: ['/host:/container[:ro]']
        :param ports: zusaetzliche Portweiterleitungen ['Host:Container']
        :param ctor_args: Konstruktor-Parameter des Runners im Container
        """
        super().__init__(type)
        self.entry = entry
        self.spec = spec
        # pip-Anforderungen nur zeilenweise trennen ("paket>=1,<2" enthaelt Kommas)
        self.packages = parse_lines(packages, comma=False)
        self.mount_lines = parse_lines(mounts)
        self.port_lines = parse_lines(ports)
        self.mounts = parse_mounts(self.mount_lines)
        self.ports = parse_ports(self.port_lines)
        self.ctor_args = ctor_args or {}
        self.image = 'nxrunner-' + ''.join(c if c.isalnum() or c in '-_.' else '-' for c in entry.lower())
        self.container = None
        self.host_port = None
        self.state = 'neu'
        self.message = ''
        self.build_log = []
        self.lock = threading.Lock()
        self.thread = None
        # nur die Ops des Proxys, alle anderen Jobs gehen an den Runner im Container
        self.define_sig(d.PStr('op', 'docker_status')).set_description(
            "Zustand des Containers (state, message, image, port, mounts, ports, packages, spec)").r(
            success='bool', message='str', value='dict')
        for op, desc in (('docker_start', "Container starten"), ('docker_restart', "Container neu starten"),
                         ('docker_rebuild', "Image neu bauen und Container starten")):
            self.define_sig(d.PStr('op', op)).set_description(desc + " (im Hintergrund)").r(
                success='bool', message='str')
        self.define_sig(d.PStr('op', 'docker_stop')).set_description(
            "Container stoppen und entfernen").r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'docker_logs')).set_description(
            "Logs des Containers bzw. Build-Log").r(success='bool', message='str', value='str')
        DockerProxy.instances.add(self)
        if start:
            self.start_async()

    # --- Docker -------------------------------------------------------------------------------

    # wird versucht, wenn DOCKER_HOST nicht gesetzt und der Standard-Socket nicht erreichbar ist
    # (z.B. dockerd in der WSL mit -H tcp://localhost:2375)
    FALLBACK_HOSTS = ['tcp://localhost:2375']

    @classmethod
    def client(cls):
        import docker
        try:
            client = docker.from_env()
            client.ping()
            return client
        except docker.errors.DockerException as e:
            if os.environ.get('DOCKER_HOST'):
                raise
            for host in cls.FALLBACK_HOSTS:
                try:
                    client = docker.DockerClient(base_url=host)
                    client.ping()
                    return client
                except docker.errors.DockerException:
                    pass
            raise RuntimeError(f'Docker nicht erreichbar (Socket und {", ".join(cls.FALLBACK_HOSTS)}), '
                               f'ggf. DOCKER_HOST setzen: {e}')

    def local_sources(self) -> dict:
        """ Pakete, die hier aus einem Quellbaum laufen: {name: verzeichnis}, kommen als Quellcode ins Image """
        res = {}
        for p in self.packages:
            name = requirement_name(p)
            root = local_source(name) if name else None
            if root:
                res[name] = root
        return res

    def dockerfile(self, local=None) -> str:
        local = self.local_sources() if local is None else local
        remote = [p for p in self.packages if requirement_name(p) not in local]
        packages = ' '.join(json.dumps(p) for p in remote)
        # pip braucht git fuer Anforderungen wie "name @ git+https://..." (im slim-Image nicht enthalten)
        git = ('RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*\n'
               if any('git+' in p for p in remote) else '')
        # lokale Quellen zuletzt: ueberschreiben ggf. die per Abhaengigkeit von PyPI geholte Version
        src = ''.join(f'COPY src/{name} /src/{name}\n' for name in local)
        if local:
            src += 'RUN pip install --no-cache-dir ' + ' '.join(f'/src/{name}' for name in local) + '\n'
        return (f'FROM {BASE_IMAGE}\n'
                'ENV PYTHONUNBUFFERED=1 SERVER=waitress\n'
                + git +
                f'RUN pip install --no-cache-dir flask waitress {packages}\n'
                + src +
                'COPY nxrunner_start.py /nxrunner_start.py\n'
                f'EXPOSE {CONTAINER_PORT}\n'
                'CMD ["python", "/nxrunner_start.py"]\n')

    def build(self, client):
        """ Image bauen, Dockerfile und Startskript als Build-Kontext im Speicher """
        local = self.local_sources()
        self.state, self.message = 'baut', f'Image {self.image} wird gebaut (pip install {" ".join(self.packages)})'
        if local:
            self.message += '; lokal: ' + ', '.join(f'{n} ({d})' for n, d in local.items())
        context = io.BytesIO()
        with tarfile.open(fileobj=context, mode='w') as tar:
            for name, content in (('Dockerfile', self.dockerfile(local)), ('nxrunner_start.py', START_SCRIPT)):
                data = content.encode()
                info = tarfile.TarInfo(name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
            for name, root in local.items():
                for f in source_files(root):
                    tar.add(os.path.join(root, f), arcname=f'src/{name}/' + f.replace(os.sep, '/'), recursive=False)
        context.seek(0)
        self.build_log = []
        for chunk in client.api.build(fileobj=context, custom_context=True, tag=self.image, rm=True, decode=True):
            if 'stream' in chunk:
                self.build_log.append(chunk['stream'].rstrip())
                self.build_log = self.build_log[-200:]
            if 'error' in chunk:
                raise RuntimeError('Image bauen fehlgeschlagen: ' + chunk['error'])

    def start(self, rebuild=False):
        import docker
        with self.lock:
            self.stop_container()
            client = self.client()
            try:
                if rebuild:
                    raise docker.errors.ImageNotFound('neu bauen')
                client.images.get(self.image)
            except docker.errors.ImageNotFound:
                self.build(client)
            self.state, self.message = 'startet', 'Container wird gestartet'
            self.host_port = free_port()
            self.container = client.containers.run(
                # nicht auto_remove: bei Startfehlern sollen die Logs noch lesbar sein (entfernt wird in stop_container)
                self.image, detach=True, name=f'{self.image}-{self.host_port}',
                ports={f'{CONTAINER_PORT}/tcp': ('127.0.0.1', self.host_port), **self.ports},
                volumes=self.mounts,
                environment={'NX_RUNNER_SPEC': self.spec, 'NX_RUNNER_ARGS': json.dumps(self.ctor_args)},
                labels={'nxrunner': self.type})
            self.wait_ready()
            self.state, self.message = 'laeuft', f'Container {self.container.name}, Port {self.host_port}'

    def wait_ready(self, timeout=120):
        """ warten, bis der Runner im Container auf HTTP antwortet """
        end = time.time() + timeout
        while time.time() < end:
            self.container.reload()
            if self.container.status in ('exited', 'dead'):
                # Logs fuer die Fehlermeldung sichern, danach wird der Container entfernt
                logs = self.logs(40)
                self.build_log = logs.splitlines()
                self.stop_container()
                last = next((line for line in reversed(self.build_log) if line.strip()), '')
                raise RuntimeError(f'Container hat sich beim Start beendet: {last} (Logs siehe unten bzw. op=docker_logs)')
            try:
                requests.get(self.url(), timeout=2)
                return
            except requests.RequestException:
                time.sleep(1)
        raise RuntimeError('Runner im Container antwortet nicht')

    def start_async(self, rebuild=False):
        if self.thread is not None and self.thread.is_alive():
            return False

        def run():
            try:
                self.start(rebuild)
            except Exception as e:
                self.state, self.message = 'fehler', f'{type(e).__name__}: {e}'
                self.error('DockerProxy ' + self.type + ': ' + self.message)

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()
        return True

    def stop_container(self):
        """ Container stoppen und entfernen """
        if self.container is not None:
            try:
                self.container.stop(timeout=5)
            except Exception:
                pass
            try:
                self.container.remove(force=True)
            except Exception:
                pass
            self.container = None
        self.host_port = None
        self.state, self.message = 'gestoppt', ''

    def logs(self, tail=200):
        """ Logs des laufenden Containers, sonst die gesicherten (Build-Log bzw. Logs nach einem Startfehler) """
        try:
            if self.container:
                return self.container.logs(tail=tail).decode('utf-8', errors='replace')
            return '\n'.join(self.build_log[-tail:])
        except Exception as e:
            return str(e)

    def url(self):
        return f'http://127.0.0.1:{self.host_port}/'

    # --- Jobs ---------------------------------------------------------------------------------

    def status(self):
        return {'state': self.state, 'message': self.message, 'image': self.image, 'port': self.host_port,
                'mounts': self.mount_lines, 'ports': self.port_lines, 'packages': self.packages, 'spec': self.spec}

    def execute(self, data):
        op = data.get('op', '')
        if op == 'docker_status':
            return self.success('ok', value=self.status())
        if op in ('docker_start', 'docker_restart', 'docker_rebuild'):
            started = self.start_async(rebuild=op == 'docker_rebuild')
            return self.success('wird gestartet' if started else 'Start laeuft bereits', ui_reload=True)
        if op == 'docker_stop':
            with self.lock:
                self.stop_container()
            return self.success('gestoppt', ui_reload=True)
        if op == 'docker_logs':
            return self.success('ok', value=self.logs() or '\n'.join(self.build_log))
        # Seiten (page=index, aber auch z.B. stats, stdout, run): Bedienung des Containers immer oben anzeigen
        is_page = bool(data.get('page'))
        if self.state != 'laeuft':
            if is_page:
                return self.success(content=self.html_controls())
            return self.fail(f'Container {self.state}: {self.message}')
        result = self.forward(data)
        if is_page and isinstance(result, dict):
            if 'content' in result:
                result['content'] = self.html_controls() + str(result['content'])
            else:
                # Runner im Container liefert keine Seite: trotzdem Bedienung anzeigen, darunter seine Antwort
                result = self.success(content=self.html_controls() + '<pre>' + html.escape(json.dumps(result, indent=2, default=str)) + '</pre>')
        return result

    def forward(self, data):
        """ Job per HTTP an den Runner im Container (wie ProxyRunner) """
        try:
            payload = {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in data.items()}
            res = requests.post(self.url(), data=payload, timeout=600)
            try:
                return res.json()
            except ValueError:
                return self.success(content=res.text) if data.get('page') else self.fail('Keine JSON-Antwort: ' + res.text[:500])
        except requests.RequestException as e:
            return self.fail(f'Container nicht erreichbar: {e}')

    # --- Seite --------------------------------------------------------------------------------

    def html_controls(self) -> str:
        """ Kopf mit Status und Bedienung, ueber der Seite des Runners im Container """
        p = b.Page(owner=self)
        with p.section(h='Docker: ' + self.image):
            p.div(f'Status: <b>{html.escape(self.state)}</b> {html.escape(self.message)}')
            for title, lines in (('Mounts', self.mount_lines), ('Ports', self.port_lines)):
                if lines:
                    p.div(f'{title}: ' + html.escape(', '.join(lines)))
            for title, op in (('Stop', 'docker_stop'), ('Neustart', 'docker_restart'), ('Neu bauen', 'docker_rebuild')):
                p(self.action_btn({'title': title, 'type': self.type, 'op': op}))
            p(self.action_btn_parametric('Logs', {'type': self.type, 'op': 'docker_logs'},
                                         on_success='function(r) { document.getElementById("docker_logs").textContent = r.value; }'))
            p.pre('', id='docker_logs')
            if self.state in ('baut', 'startet'):
                # waehrend Bauen/Starten regelmaessig neu laden
                p.script('setTimeout(function() { location.reload(); }, 3000);')
            if self.state in ('baut', 'startet', 'fehler') and self.build_log:
                # Build-Log bzw. bei Startfehlern die gesicherten Logs des Containers
                p.pre(html.escape('\n'.join(self.build_log[-40:])))
        return str(p)


@atexit.register
def stop_all():
    for proxy in list(DockerProxy.instances):
        proxy.stop_container()
