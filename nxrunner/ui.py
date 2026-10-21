
import html
import json
import os
import re
import subprocess
import sys
import time
from importlib import resources

from nwebclient import runner as r
from nwebclient import base
from nwebclient import web
from nwebclient import util as u

from nxrunner import registry


def load_resource(name):
    return resources.files('nxrunner').joinpath('resources').joinpath(name).read_text(encoding='utf-8')


class NxRunner(r.AutoDispatcher):
    """
      Dispatcher von nxrunner mit der neuen Oberflaeche /pysys/ui (Runner-Seiten und Registry).
      op=registry [refresh]          installierte Runner (Entry-Points nweb_runner) mit Status
      op=registry_load name [runner_type] [arg_<param>=...]   Runner aus der Registry laden
      op=registry_install name [runner_type] [directory] [overwrite] [preview] [arg_<param>=...]
                                     Config <directory>/<runner_type>.yml schreiben und Runner laden
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

    TYPE_RE = re.compile(r'^[A-Za-z0-9_.\-]+$')

    def install_plan(self, data):
        """
          Konfiguration fuer die Installation eines Registry-Runners: {name, runner_type, spec, path, content}
          spec ist 'modul:Klasse' bzw. mit Parametern 'modul:Klasse(a=1, b='x')' (wie in runners einer Config)
        """
        name = str(data.get('name', '')).strip()
        ep = registry.entry(name)
        if ep is None:
            raise ValueError('Runner nicht in der Registry: ' + name)
        key = str(data.get('runner_type') or name).strip()
        if not self.TYPE_RE.match(key):
            raise ValueError('Name (type) darf nur Buchstaben, Ziffern, _ . - enthalten: ' + key)
        ctor_args = {k[4:]: registry.parse_value(v) for k, v in data.items()
                     if k.startswith('arg_') and str(v).strip() != ''}
        spec = ep.value.strip()
        if ctor_args:
            if '(' in spec:
                raise ValueError('Der Entry-Point enthaelt bereits Parameter: ' + spec)
            spec += '(' + ', '.join(f'{k}={v!r}' for k, v in ctor_args.items()) + ')'
        directory = str(data.get('directory') or u.Args.CONFIG_D).strip()
        # JSON-Strings sind gueltiges YAML (Spec kann Anfuehrungszeichen, : und # enthalten)
        content = f'# installiert ueber die Registry von nxrunner ({name})\nrunners:\n  {key}: {json.dumps(spec)}\n'
        return {'name': name, 'runner_type': key, 'spec': spec, 'path': os.path.join(directory, key + '.yml'),
                'content': content}

    def execute_registry_install(self, data):
        """
          Runner installieren: Config <directory>/<runner_type>.yml (Standard: Args.CONFIG_D) schreiben und den Runner
          sofort laden (ohne Neustart). preview=1: nur Pfad und Inhalt liefern, overwrite=1: bestehende Datei ersetzen.
        """
        try:
            plan = self.install_plan(data)
        except ValueError as e:
            return self.fail(str(e))
        exists = os.path.exists(plan['path'])
        if str(data.get('preview', '')) in ('1', 'true'):
            warning = web.alert_warn(f'{html.escape(plan["path"])} existiert bereits. Beim Installieren mit '
                                     f'&bdquo;&Uuml;berschreiben&ldquo; wird die Datei vollst&auml;ndig ersetzt, '
                                     f'weitere Eintr&auml;ge darin gehen verloren.') if exists else ''
            return self.success('Vorschau', exists=exists, loaded=plan['runner_type'] in self.entries,
                                warning=warning, **plan)
        if plan['runner_type'] in self.entries:
            return self.fail(f'Es gibt bereits einen Runner {plan["runner_type"]}, anderen Namen (type) waehlen', **plan)
        if exists and str(data.get('overwrite', '')) not in ('1', 'true'):
            return self.fail(f'{plan["path"]} existiert bereits (Ueberschreiben ankreuzen)', exists=True, **plan)
        # zuerst erzeugen: schlaegt das fehl (z.B. Modul fehlt), wird keine Config geschrieben
        try:
            runner = self.create_class(plan['spec'])
        except ModuleNotFoundError as e:
            return self.fail(f'Modul fehlt: {e.name}, nichts installiert', **plan)
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}, nichts installiert', **plan)
        try:
            os.makedirs(os.path.dirname(plan['path']) or '.', exist_ok=True)
            with open(plan['path'], 'w', encoding='utf-8') as f:
                f.write(plan['content'])
        except OSError as e:
            return self.fail(f'Config nicht geschrieben: {e}', **plan)
        res = self.load_runner(plan['runner_type'], runner)
        if not res.get('success'):
            return self.fail(f'{plan["path"]} geschrieben, Laden fehlgeschlagen: {res.get("message")}', **plan)
        return self.success(f'{plan["path"]} geschrieben und {plan["runner_type"]} geladen', cls=type(runner).__name__,
                            **plan)

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
        elif view == 'install':
            try:
                entry = self.registry_entry(str(params.get('name', '')))
            except ValueError as e:
                entry = {'error_page': str(e)}
            # Werte aus dem Formular der Registry (runner_type, arg_*) vorbelegen
            extra['install'] = {'entry': entry, 'config_d': u.Args.CONFIG_D,
                                'values': {k: v for k, v in params.items() if k == 'runner_type' or k.startswith('arg_')}}
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

