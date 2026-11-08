"""
Deployments: Git-Checkouts auf entfernten Rechnern per SSH pruefen und aktualisieren

    Config (runners):
        deployments: nxrunner.projects:Deployments

    Args (z.B. nweb.yml), Schluessel 'deployment', ein Eintrag oder eine Liste:
        deployment:
          - sshhost: raspi5
            path: /home/pi/nwebclient
            update_cmd: pip install -e .          # str oder Liste, optional
          - sshhost: handy
            path: /data/app
            mobile: true                          # nicht immer online, letzter Stand bleibt sichtbar

    Name eines Deployments: <sshhost>-<letzter Bestandteil von path>, z.B. raspi5-nwebclient

    Seite Hosts: je SSH-Host der Deployments die Ausgaben der Befehle aus dem Arg 'hostinfo':
        hostinfo:
          python_version: python -V
          df: df -h / | tail -n 1

    Job:
        {"type": "deployments", "op": "check", "name": "raspi5-nwebclient"}
        {"type": "deployments", "op": "update", "name": "raspi5-nwebclient"}
        {"type": "deployments", "op": "hostinfo", "host": "raspi5"}

Der Status (Version lokal/origin) wird, wenn vorhanden, in der nweb-Persistence (Setting deployments.status.<name>)
gespeichert, ebenso die Host-Infos mit Zeitstempel (Setting deployments.host.<sshhost>), sodass bei mobilen Geraeten, die gerade offline sind, der letzte bekannte Stand angezeigt wird.
SSH laeuft mit BatchMode (Schluessel/Agent noetig, keine Passwortabfrage).
"""
import concurrent.futures
import html
import json
import posixpath
import shlex
import subprocess
import time

from nwebclient import runner as r
from nwebclient import base
from nwebclient import util as u
from nwebclient import web as w
from nwebclient.runner import op


def esc(value):
    return html.escape('' if value is None else str(value), quote=True)


def as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on', 'ja')


def fmt_time(ts):
    return time.strftime('%Y-%m-%d %H:%M', time.localtime(ts)) if ts else '-'


class Deployment:
    """ Ein Git-Checkout auf einem SSH-Host """

    def __init__(self, sshhost, path, update_cmd=None, mobile=False, **kwargs):
        self.sshhost = str(sshhost).strip()
        self.path = str(path).strip()
        if not self.sshhost or not self.path:
            raise ValueError(f"Deployment braucht sshhost und path: {sshhost!r} {path!r}")
        if update_cmd is None or update_cmd == '':
            update_cmd = []
        self.update_cmds = [str(c) for c in (update_cmd if isinstance(update_cmd, (list, tuple)) else [update_cmd])]
        self.mobile = as_bool(mobile)
        self.project_name = posixpath.basename(self.path.rstrip('/')) or 'root'
        self.name = self.sshhost + '-' + self.project_name

    def remote_cmd(self, *cmds):
        """ Shell-Befehl fuer den entfernten Rechner, im Verzeichnis path """
        return ' && '.join(['cd ' + shlex.quote(self.path)] + list(cmds))

    def update_cmd_line(self):
        return self.remote_cmd('git pull', *self.update_cmds)

    def to_dict(self):
        return {'name': self.name, 'project_name': self.project_name, 'sshhost': self.sshhost, 'path': self.path,
                'update_cmd': self.update_cmds,
                'mobile': self.mobile}


class Deployments(r.BaseJobExecutor):
    """
      Deployments (Git-Checkouts per SSH) aus dem Arg 'deployment': Version mit origin vergleichen, aktualisieren
      (git pull und update_cmd im Verzeichnis path).
    """

    ARG_KEY = 'deployment'
    HOSTINFO_KEY = 'hostinfo'
    HOSTINFO_MARK = '__NX_HOSTINFO__ '

    # Ausgabe der Statuspruefung, je Zeile KEY=Wert
    STATUS_SCRIPT = ('git fetch -q origin 2>/dev/null; '
                     'echo "BRANCH=$(git rev-parse --abbrev-ref HEAD)"; '
                     'echo "LOCAL=$(git rev-parse HEAD)"; '
                     'echo "REMOTE=$(git rev-parse @{u} 2>/dev/null)"; '
                     'echo "BEHIND=$(git rev-list --count HEAD..@{u} 2>/dev/null)"; '
                     'echo "AHEAD=$(git rev-list --count @{u}..HEAD 2>/dev/null)"; '
                     'echo "DIRTY=$(git status --porcelain | wc -l)"; '
                     'echo "SUBJECT=$(git log -1 --format=%s)"; '
                     'echo "DATE=$(git log -1 --format=%cI)"')

    def __init__(self, type='deployments', ssh='ssh', connect_timeout=5, timeout=60, update_timeout=600):
        super().__init__(type)
        self.ssh_bin = ssh
        self.connect_timeout = int(connect_timeout)
        self.timeout = int(timeout)
        self.update_timeout = int(update_timeout)
        self.status_cache = {}
        self.last_output = ''
        self.last_cmd = ''
        self.define_vars('connect_timeout', 'timeout', 'update_timeout')

    # ------------------------------------------------------------------
    # Konfiguration
    # ------------------------------------------------------------------

    def args_value(self, key=None):
        args = getattr(self.getRoot(), 'args', None)
        if args is None or not hasattr(args, 'get'):
            args = u.Args()
        value = args.get(key or self.ARG_KEY, None)
        if isinstance(value, str) and value.strip()[:1] in ('[', '{'):
            value = json.loads(value)
        return value

    def deployments(self) -> list:
        """ Deployments aus dem Arg 'deployment' (dict, Liste von dicts oder {name: dict}) """
        value = self.args_value()
        if not value:
            return []
        if isinstance(value, dict):
            value = [value] if 'sshhost' in value else list(value.values())
        res = []
        for entry in value:
            try:
                res.append(Deployment(**entry))
            except (TypeError, ValueError) as e:
                self.error(f"Deployment ungueltig: {entry!r} ({e})")
        return res

    def deployment(self, name) -> Deployment:
        for dep in self.deployments():
            if dep.name == name:
                return dep
        raise KeyError(f"Deployment {name} nicht gefunden")

    def hosts(self) -> dict:
        """ SSH-Hosts der Deployments {sshhost: mobile}, mobil wenn eines der Deployments mobil ist """
        res = {}
        for dep in self.deployments():
            res[dep.sshhost] = res.get(dep.sshhost, False) or dep.mobile
        return res

    def hostinfo_cmds(self) -> dict:
        """ Befehle aus dem Arg 'hostinfo' {schluessel: befehl} """
        value = self.args_value(self.HOSTINFO_KEY)
        return {str(k): str(v) for k, v in value.items()} if isinstance(value, dict) else {}

    # ------------------------------------------------------------------
    # Status (Speicher + nweb-Persistence)
    # ------------------------------------------------------------------

    def load(self, key) -> dict:
        """ gespeicherter Wert (dict) zum Setting-Schluessel key, z.B. status.<name> """
        if key not in self.status_cache:
            try:
                stored = self.setting(key, None)
                self.status_cache[key] = json.loads(stored) if stored else {}
            except Exception as e:
                self.error(f"{key} nicht lesbar: {e}")
                self.status_cache[key] = {}
        return self.status_cache[key]

    def store(self, key, value: dict):
        self.status_cache[key] = value
        try:
            self.setting_set(key, value)
        except Exception as e:
            self.error(f"{key} nicht gespeichert: {e}")

    def get_status(self, name) -> dict:
        return self.load('status.' + name)

    def save_status(self, name, status: dict):
        self.store('status.' + name, status)

    def get_hostinfo(self, host) -> dict:
        return self.load('host.' + host)

    # ------------------------------------------------------------------
    # SSH
    # ------------------------------------------------------------------

    def ssh(self, sshhost, cmd, timeout=None):
        """ Fuehrt cmd auf sshhost aus, Ergebnis (returncode, ausgabe); 255 = Verbindung fehlgeschlagen """
        args = [self.ssh_bin, '-o', 'BatchMode=yes', '-o', f'ConnectTimeout={self.connect_timeout}', sshhost, cmd]
        try:
            res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True,
                                 timeout=timeout or self.timeout)
            return res.returncode, res.stdout
        except FileNotFoundError:
            return 255, f"{self.ssh_bin} nicht gefunden"
        except subprocess.TimeoutExpired:
            return 124, f"Zeitüberschreitung nach {timeout or self.timeout} s"

    @staticmethod
    def parse_status(output) -> dict:
        values = {}
        for line in output.splitlines():
            key, sep, value = line.partition('=')
            if sep and key.isupper():
                values[key.lower()] = value.strip()
        for key in ('behind', 'ahead', 'dirty'):
            values[key] = int(values[key]) if values.get(key, '').isdigit() else None
        return values

    def check(self, dep: Deployment) -> dict:
        """ Prueft die Version von dep gegen origin und speichert den Status """
        code, out = self.ssh(dep.sshhost, dep.remote_cmd(self.STATUS_SCRIPT))
        status = dict(self.get_status(dep.name))
        status['checked'] = time.time()
        if code == 255 or code == 124:
            status.update(online=False, error=out.strip())
        else:
            values = self.parse_status(out)
            if code != 0 or not values.get('local'):
                status.update(online=True, error=out.strip() or f"Exit-Code {code}")
            else:
                status.update(values, online=True, error='', last_online=status['checked'])
        self.save_status(dep.name, status)
        return status

    def check_many(self, deps: list) -> dict:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            return dict(zip([d.name for d in deps], pool.map(self.check, deps)))

    def update(self, dep: Deployment) -> dict:
        """ git pull und update_cmd im Verzeichnis path, danach erneut pruefen """
        cmd = dep.update_cmd_line()
        code, out = self.ssh(dep.sshhost, cmd, timeout=self.update_timeout)
        self.last_cmd = f"ssh {dep.sshhost} {cmd}"
        self.last_output = out
        self.info(f"{self.last_cmd}: {'ok' if code == 0 else 'Fehler ' + str(code)}")
        status = self.check(dep)
        status['last_update'] = {'time': time.time(), 'ok': code == 0, 'code': code, 'output': out[-4000:]}
        self.save_status(dep.name, status)
        return status

    # ------------------------------------------------------------------
    # Host-Infos
    # ------------------------------------------------------------------

    def hostinfo_script(self, cmds: dict):
        """ ein Shell-Skript fuer alle Befehle, vor jeder Ausgabe eine Markierungszeile mit dem Schluessel """
        return '; '.join(f"echo {shlex.quote(self.HOSTINFO_MARK + key)}; ({cmd}) 2>&1" for key, cmd in cmds.items())

    def parse_hostinfo(self, output) -> dict:
        values, key = {}, None
        for line in output.splitlines():
            if line.startswith(self.HOSTINFO_MARK):
                key = line[len(self.HOSTINFO_MARK):].strip()
                values[key] = []
            elif key is not None:
                values[key].append(line)
        return {k: '\n'.join(v).strip() for k, v in values.items()}

    def query_host(self, host) -> dict:
        """ Fragt die hostinfo-Werte von host ab, bei offline bleiben die alten Werte mit ihrem Zeitstempel """
        cmds = self.hostinfo_cmds()
        info = dict(self.get_hostinfo(host))
        info['checked'] = now = time.time()
        code, out = self.ssh(host, self.hostinfo_script(cmds)) if cmds else (0, '')
        if code in (255, 124):
            info.update(online=False, error=out.strip())
        else:
            values = dict(info.get('values') or {})
            for key, value in self.parse_hostinfo(out).items():
                values[key] = {'value': value, 'time': now}
            info.update(online=True, error='', last_online=now, values=values)
        self.store('host.' + host, info)
        return info

    def query_hosts(self, hosts: list) -> dict:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            return dict(zip(hosts, pool.map(self.query_host, hosts)))

    # ------------------------------------------------------------------
    # Operationen
    # ------------------------------------------------------------------

    @op("Host-Infos (Arg hostinfo) eines SSH-Hosts (leer = alle) abfragen", success={'hosts': 'dict'},
        name='hostinfo')
    def hostinfo_op(self, host: str = ''):
        hosts = list(self.hosts())
        if host and host not in hosts:
            return self.fail(f"Host {host} nicht gefunden")
        result = self.query_hosts([host] if host else hosts)
        offline = [h for h, i in result.items() if not i.get('online')]
        msg = f"{len(result)} abgefragt" + (f", offline: {', '.join(offline)}" if offline else '')
        return self.success(msg, hosts=result, ui_reload=True)

    @op("Deployments mit letztem bekannten Status", success={'deployments': 'list'}, name='list')
    def list_op(self):
        rows = [{**dep.to_dict(), 'status': self.get_status(dep.name)} for dep in self.deployments()]
        return self.success(deployments=rows)

    @op("Version eines Deployments (leer = alle) mit git remote origin vergleichen",
        success={'status': 'dict'}, name='check')
    def check_op(self, name: str = ''):
        try:
            deps = self.deployments() if not name else [self.deployment(name)]
        except KeyError as e:
            return self.fail(e.args[0])
        result = self.check_many(deps)
        offline = [n for n, s in result.items() if not s.get('online')]
        msg = f"{len(result)} geprüft" + (f", offline: {', '.join(offline)}" if offline else '')
        return self.success(msg, status=result, ui_reload=True)

    @op("Deployment aktualisieren: git pull und update_cmd im Verzeichnis path", success={'status': 'dict'},
        name='update')
    def update_op(self, name: str = ''):
        try:
            dep = self.deployment(name)
        except KeyError as e:
            return self.fail(e.args[0])
        status = self.update(dep)
        upd = status['last_update']
        if not upd['ok']:
            return self.fail(f"Aktualisieren von {name} fehlgeschlagen (Exit-Code {upd['code']})",
                             output=upd['output'], status=status, ui_reload=True)
        return self.success(f"{name} aktualisiert", output=upd['output'], status=status, ui_reload=True)

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def btn(self, title, op_name, hint, **params):
        return self.action_btn(dict(type=self.type, op=op_name, title=title, **params), hint=hint)

    def nav(self, p: base.Page):
        p.div(' | '.join([w.a("Übersicht", self.link(self.part_index), hint="Alle Deployments"),
                          w.a("Hosts", self.link(self.part_hosts), hint="Infos je SSH-Host (Arg hostinfo)"),
                          w.a("Letzte Ausgabe", self.link(self.part_output), hint="Ausgabe des letzten Updates")]))

    def part_index(self, p: base.Page, params={}):
        self.nav(p)
        deps = self.deployments()
        with p.section(h="Deployments"):
            if not deps:
                p(w.alert_info(f"Keine Deployments, im Arg '{self.ARG_KEY}' eintragen (sshhost, path, "
                               f"update_cmd, mobile)."))
                return
            rows = [{'dep': dep, 'status': self.get_status(dep.name)} for dep in deps]
            p.grid(rows, self.grid_cols(), params, search=True)
            p.div(self.btn("Alle prüfen", 'check', "Vergleicht alle Deployments per SSH mit git remote origin"))

    def deployments_for(self, project_names) -> list:
        """ Deployments, deren project_name in project_names (str oder Sammlung) liegt """
        names = {project_names} if isinstance(project_names, str) else set(project_names)
        return [dep for dep in self.deployments() if dep.project_name in names]

    def part_project_deployments(self, p: base.Page, project_names, params={}):
        """ Section mit den Deployments eines Projekts, z.B. fuer den Index von GitRepo; nichts ohne Treffer """
        deps = self.deployments_for(project_names)
        if not deps:
            return
        with p.section(h="Deployments"):
            rows = [{'dep': dep, 'status': self.get_status(dep.name)} for dep in deps]
            cols = [c for c in self.grid_cols() if c['field'] != 'project']
            p.grid(rows, cols, params)
            p.div(w.a("Alle Deployments", self.link(self.part_index), hint=f"Seite des Runners {self.type}"))

    def grid_cols(self):
        col = w.Grid.col
        return [
            col('name', "Name", html=True, value=self.col_name, sort=lambda e: e['dep'].name),
            col('project', "Project", html=True, value=self.col_project, sort=lambda e: e['dep'].project_name),
            col('mobile', "Mobil", value=lambda e: 'ja' if e['dep'].mobile else ''),
            col('branch', "Branch", value=lambda e: e['status'].get('branch', '')),
            col('local', "Version", html=True, value=self.col_version),
            col('remote', "origin", value=lambda e: (e['status'].get('remote') or '')[:8]),
            col('state', "Status", html=True, value=self.col_state, sort=lambda e: self.state(e['status'])[0]),
            col('checked', "Geprüft", html=True, value=self.col_checked,
                sort=lambda e: e['status'].get('checked') or 0),
            col('actions', '', html=True, sortable=False, value=self.col_actions),
        ]

    def col_name(self, e):
        dep = e['dep']
        return w.a(esc(dep.name), self.link(self.part_deployment, name=dep.name), hint=f"{dep.sshhost}:{dep.path}")

    def runner_names(self) -> set:
        """ Namen der Runner im Dispatcher (geladen oder nur registriert, ohne sie zu erzeugen) """
        disp = self.getParentClass(r.LazyDispatcher)
        if disp is None:
            disp = getattr(self.getRoot(), 'jobexecutor', None)
        return set(getattr(disp, 'entries', None) or getattr(disp, 'instances', None) or {})

    def col_project(self, e, runners=None):
        name = e['dep'].project_name
        if name != self.type and name in (self.runner_names() if runners is None else runners):
            return w.a(esc(name), f'?type={name}&{name}=index', hint=f"Seite des Runners {name}")
        return esc(name)

    @staticmethod
    def col_version(e):
        s = e['status']
        if not s.get('local'):
            return ''
        return f'<span title="{esc(s.get("subject", ""))} ({esc(s.get("date", ""))})">{esc(s["local"][:8])}</span>'

    @staticmethod
    def state(s: dict):
        """ (Sortierschluessel, Text, CSS-Klasse) des Versionsstands """
        if not s.get('checked') or (not s.get('local') and not s.get('online')):
            return 5, "unbekannt", ''
        if not s.get('local'):
            return 4, "Fehler", 'warn'
        if not s.get('remote'):
            return 3, "kein origin", 'warn'
        if s.get('behind'):
            return 2, f"{s['behind']} zurück", 'warn'
        if s.get('local') != s.get('remote'):
            return 1, f"{s.get('ahead') or 0} voraus", 'tag'
        return 0, "aktuell", 'ok'

    def col_state(self, e):
        s = e['status']
        _, text, css = self.state(s)
        res = f'<span class="badge {css}" title="{esc(s.get("error", ""))}">{esc(text)}</span>'
        if s.get('dirty'):
            res += f' <span class="badge warn" title="Lokale Änderungen im Arbeitsverzeichnis">{s["dirty"]} geändert</span>'
        if s.get('checked') and not s.get('online'):
            res += ' <span class="badge" title="Bei der letzten Prüfung nicht erreichbar">offline</span>'
        return res

    @staticmethod
    def col_checked(e):
        s = e['status']
        res = esc(fmt_time(s.get('checked')))
        if s.get('checked') and not s.get('online'):
            res += f'<br><small title="Stand der angezeigten Version">online: {esc(fmt_time(s.get("last_online")))}</small>'
        return res

    def col_actions(self, e):
        name = e['dep'].name
        return (self.btn("Prüfen", 'check', "Version per SSH mit origin vergleichen", name=name) +
                self.btn("Aktualisieren", 'update', "git pull und update_cmd auf dem Host ausführen", name=name))

    def part_deployment(self, p: base.Page, params={}):
        self.nav(p)
        try:
            dep = self.deployment(params.get('name', ''))
        except KeyError as e:
            p(w.alert_error(esc(e.args[0])))
            return
        s = self.get_status(dep.name)
        with p.section(h=f"Deployment {esc(dep.name)}"):
            for title, value in (("Projekt", dep.project_name), ("SSH-Host", dep.sshhost), ("Pfad", dep.path), ("Mobil", 'ja' if dep.mobile else 'nein'),
                                 ("Update", dep.update_cmd_line()), ("Branch", s.get('branch')),
                                 ("Version", s.get('local')), ("origin", s.get('remote')),
                                 ("Commit", f"{s.get('subject', '')} ({s.get('date', '')})" if s.get('local') else ''),
                                 ("Status", self.state(s)[1]), ("Geprüft", fmt_time(s.get('checked'))),
                                 ("Zuletzt online", fmt_time(s.get('last_online'))), ("Fehler", s.get('error'))):
                p.prop(title, esc(value or '-'))
            p.div(self.col_actions({'dep': dep}))
        self.part_last_update(p, s.get('last_update'))

    def part_last_update(self, p: base.Page, upd):
        if not upd:
            return
        with p.section(h=f"Letztes Update {fmt_time(upd.get('time'))} ({'ok' if upd.get('ok') else 'Fehler'})"):
            p.pre(esc(upd.get('output') or ''))

    def part_output(self, p: base.Page, params={}):
        self.nav(p)
        with p.section(h="Letzte Ausgabe"):
            p.div(esc(self.last_cmd or "Seit dem Start wurde nichts aktualisiert."))
            p.pre(esc(self.last_output))

    # ------------------------------------------------------------------
    # UI: Hosts
    # ------------------------------------------------------------------

    def part_hosts(self, p: base.Page, params={}):
        self.nav(p)
        hosts = self.hosts()
        cmds = self.hostinfo_cmds()
        with p.section(h="Hosts"):
            if not hosts:
                p(w.alert_info(f"Keine Hosts, Deployments im Arg '{self.ARG_KEY}' eintragen."))
                return
            if not cmds:
                p(w.alert_info(f"Keine Befehle, im Arg '{self.HOSTINFO_KEY}' eintragen (schluessel: befehl)."))
            rows = [{'host': h, 'mobile': m, 'info': self.get_hostinfo(h)} for h, m in hosts.items()]
            p.grid(rows, self.host_cols(cmds), params, search=True)
            p.div(self.btn("Alle abfragen", 'hostinfo', "Fragt die Host-Infos aller Hosts per SSH ab"))

    def host_cols(self, cmds: dict):
        col = w.Grid.col
        cols = [col('host', "Host"), col('mobile', "Mobil", value=lambda e: 'ja' if e['mobile'] else '')]
        for key, cmd in cmds.items():
            cols.append(col(key, key, html=True, value=lambda e, k=key: self.col_hostvalue(e, k),
                            sort=lambda e, k=key: self.hostvalue(e, k).get('value', ''),
                            td={'title': lambda e, c=cmd: c}))
        cols += [col('checked', "Geprüft", html=True, value=self.col_host_checked,
                     sort=lambda e: e['info'].get('checked') or 0),
                 col('actions', '', html=True, sortable=False,
                     value=lambda e: self.btn("Abfragen", 'hostinfo', "Host-Infos per SSH abfragen", host=e['host']))]
        return cols

    @staticmethod
    def hostvalue(e, key) -> dict:
        return (e['info'].get('values') or {}).get(key) or {}

    def col_hostvalue(self, e, key):
        entry = self.hostvalue(e, key)
        if not entry:
            return ''
        value = esc(entry.get('value', '')).replace('\n', '<br>')
        return f'<span title="Stand {esc(fmt_time(entry.get("time")))}">{value}</span>'

    @staticmethod
    def col_host_checked(e):
        info = e['info']
        res = esc(fmt_time(info.get('checked')))
        if info.get('checked') and not info.get('online'):
            res += (f' <span class="badge" title="{esc(info.get("error", ""))}">offline</span>'
                    f'<br><small title="Stand der angezeigten Werte">online: {esc(fmt_time(info.get("last_online")))}</small>')
        return res
