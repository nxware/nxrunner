
import json
import subprocess
import sys
import time
from importlib import resources

from nwebclient import runner as r
from nwebclient import base
from nwebclient import web

from nxrunner import registry


def load_resource(name):
    return resources.files('nxrunner').joinpath('resources').joinpath(name).read_text(encoding='utf-8')


class NxRunner(r.AutoDispatcher):
    """
      Dispatcher von nxrunner mit der neuen Oberflaeche /pysys/ui (Runner-Seiten und Registry).
      op=registry [refresh]          installierte Runner (Entry-Points nweb_runner) mit Status
      op=registry_load name [runner_type] [arg_<param>=...]   Runner aus der Registry laden
      op=registry_pip name           fehlende MODULES per pip installieren
      op=registry_docker name [runner_type] [packages] [mounts] [ports] [arg_<param>=...]
                                     Runner in einem Docker-Container laden (DockerProxy)
    """

    # so lange wird das (statisch gelesene) Verzeichnis der installierten Runner zwischengespeichert
    REGISTRY_CACHE_S = 300

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.type = 'nxrunner'
        self.registry_cache = None  # (Zeitpunkt, Eintraege)

    def ui_data(self):
        instances = [{'name': name, 'cls': type(runner).__name__} for name, runner in self.instances.items()]
        classes = [{'name': name, 'cls': str(cls)} for name, cls in self.classes.items() if name not in self.instances]
        return {
            'key': self.key,
            'instances': instances,
            'classes': classes,
            'links': [
                {'title': "Dispatcher", 'url': '/pysys/runner'},
                {'title': "Runner-UI", 'url': '/pysys/runner-ui'},
                {'title': "Registry", 'url': '/pysys/ui?view=registry'},
                {'title': "Results", 'url': '/pysys/job-results'},
                {'title': "Proxy", 'url': '/pysys/create-proxy'}
            ]
        }

    def query_jumplinks(self):
        return [
            web.a("UI (v2)", '/pysys/ui'),
            *super().query_jumplinks()
        ]

    def ui_runner_page(self, params):
        """
          ?type=promptdb&promptdb=stats -> execute({type: promptdb, page: stats, ...})
        """
        t = params[self.key]
        try:
            result = self.execute({self.key: t, 'page': params.get(t, 'index'), **params})
        except Exception as e:
            result = self.fail(str(e))
        if isinstance(result, dict) and result.get('success') is not False and 'content' in result:
            return {'name': t, 'content': str(result['content'])}
        message = result.get('message', '') if isinstance(result, dict) else str(result)
        return {'name': t, 'error': str(message) or json.dumps(result, default=str)}

    # --- Registry ---------------------------------------------------------------------------

    def registry_entries(self, refresh=False):
        """ installierte Runner (Entry-Points nweb_runner) mit Status im Dispatcher """
        if refresh or self.registry_cache is None or time.time() - self.registry_cache[0] > self.REGISTRY_CACHE_S:
            self.registry_cache = (time.time(), registry.entries())
        loaded, configured = self.instances, self.classes
        return [{**e, 'state': 'geladen' if e['name'] in loaded else ('konfiguriert' if e['name'] in configured else ''),
                 'docker_packages': self.docker_packages(e)}
                for e in self.registry_cache[1]]

    @staticmethod
    def docker_packages(entry) -> list:
        """
          Vorschlag fuer pip install im Docker-Image: nwebclient, das Projekt des Runners (erster Teil des Moduls,
          Quelle aus registry.PROJECTS, sonst PyPI) und alle MODULES (im Container ist nichts vorinstalliert)
        """
        packages = [registry.pip_requirement('nwebclient')]
        project = registry.project(entry['value'])
        if project != 'nwebclient':
            packages.append(registry.pip_requirement(project))
        return packages + [m for m in entry['modules'] if m not in packages]

    def execute_registry_docker(self, data):
        """
          Runner in einem Docker-Container starten (DockerProxy) und unter type (Standard: Name des Entry-Points)
          laden. packages, mounts (Host:Container[:ro]) und ports (Host:Container) zeilenweise,
          Konstruktor-Parameter als arg_<name>.
        """
        from nxrunner.dockerproxy import DockerProxy
        name = str(data.get('name', '')).strip()
        ep = registry.entry(name)
        if ep is None:
            return self.fail('Runner nicht in der Registry: ' + name)
        key = str(data.get('runner_type') or name).strip()
        if key in self.entries:
            return self.fail(f'Es gibt bereits einen Runner {key}, anderen Namen (type) waehlen')
        ctor_args = {k[4:]: registry.parse_value(v) for k, v in data.items()
                     if k.startswith('arg_') and str(v).strip() != ''}
        try:
            proxy = DockerProxy(key, name, ep.value, data.get('packages') or self.docker_packages(self.registry_entry(name)),
                                mounts=data.get('mounts', ''), ports=data.get('ports', ''), ctor_args=ctor_args)
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')
        res = self.load_runner(key, proxy)
        if not res.get('success'):
            return res
        return self.success(f'{key}: Docker-Container wird gestartet (Image {proxy.image})', runner_type=key,
                            cls='DockerProxy')

    def registry_entry(self, name):
        for e in self.registry_entries():
            if e['name'] == name:
                return e
        raise ValueError('Runner nicht in der Registry: ' + str(name))

    def execute_registry(self, data):
        return self.success('ok', value=self.registry_entries(refresh='refresh' in data))

    def execute_registry_load(self, data):
        """
          Runner aus der Registry erzeugen und unter type (Standard: Name des Entry-Points) laden.
          Konstruktor-Parameter als arg_<name>, leere Felder werden nicht uebergeben (Standardwert).
        """
        name = str(data.get('name', '')).strip()
        ep = registry.entry(name)
        if ep is None:
            return self.fail('Runner nicht in der Registry: ' + name)
        key = str(data.get('runner_type') or name).strip()
        if key in self.entries:
            return self.fail(f'Es gibt bereits einen Runner {key}, anderen Namen (type) waehlen')
        ctor_args = {k[4:]: registry.parse_value(v) for k, v in data.items()
                     if k.startswith('arg_') and str(v).strip() != ''}
        try:
            runner = self.create_class(ep.value, ctor_args)
        except ModuleNotFoundError as e:
            return self.fail(f'Modul fehlt: {e.name} (ggf. per pip install installieren)')
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')
        res = self.load_runner(key, runner)
        if not res.get('success'):
            return res
        return self.success(f'{key} geladen', runner_type=key, cls=type(runner).__name__)

    def execute_registry_pip(self, data):
        """ fehlende MODULES eines Runners per pip installieren """
        entry = self.registry_entry(str(data.get('name', '')))
        if not entry['missing']:
            return self.success('Keine fehlenden Module')
        cmd = [sys.executable, '-m', 'pip', 'install', *entry['missing']]
        try:
            p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=900)
        except subprocess.TimeoutExpired:
            return self.fail('pip install: Zeitueberschreitung')
        output = p.stdout.decode('utf-8', errors='replace')
        self.registry_cache = None  # installierte Pakete neu einlesen
        if p.returncode != 0:
            return self.fail('pip install fehlgeschlagen', output=output[-5000:])
        return self.success('Installiert: ' + ', '.join(entry['missing']), output=output[-5000:])

    def page_ui(self, params={}):
        page = self.ui_runner_page(params) if self.key in params else None
        view = params.get('view') if page is None else None
        extra = {'view': view}
        if view == 'registry':
            extra['registry'] = self.registry_entries(refresh='refresh' in params)
        data = json.dumps({**self.ui_data(), 'page': page, **extra}, default=str).replace('</', '<\\/')
        js = r.js_functions().replace('</', '<\\/')
        return load_resource('ui.html').replace('__JS__', js).replace('__DATA__', data)

    def part_index(self, p: base.Page, params={}):
        super().part_index(p, params)
        p.div(web.a("NxRunner UI", '/pysys/ui'))

    def setupRestApp(self, app: "flask.Flask"):
        super().setupRestApp(app)
        try:
            app.add_url_rule('/pysys/ui', 'nxrunner_ui',
                             view_func=lambda: self.page_ui(self.create_request_params()),
                             methods=['GET', 'POST'])
        except Exception as e:
            self.error(str(e))

