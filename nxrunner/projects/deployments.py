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

    Hosts senden: send(url) traegt diesen Rechner bei der Deployments-Instanz unter url als mobilen Host ein
    (op add_host), z.B. automatisch beim Start: nxrunner.projects:Deployments(send_to='http://server:7070/')

    Job:
        {"type": "deployments", "op": "check", "name": "raspi5-nwebclient"}
        {"type": "deployments", "op": "update", "name": "raspi5-nwebclient"}
        {"type": "deployments", "op": "hostinfo", "host": "raspi5"}
        {"type": "deployments", "op": "send", "url": "http://server:7070/"}
        {"type": "deployments", "op": "add_host", "host": "handy", "ssh": "pi@10.0.0.5", "mobile": 1}

Der Status (Version lokal/origin) wird, wenn vorhanden, in der nweb-Persistence (Setting deployments.status.<name>)
gespeichert. Hosts (gesendete Eintraege und Host-Infos mit Zeitstempel) stehen in der Tabelle deployment_hosts,
sodass bei mobilen Geraeten, die gerade offline sind, der letzte bekannte Stand angezeigt wird.
SSH laeuft mit BatchMode (Schluessel/Agent noetig, keine Passwortabfrage).
"""
import concurrent.futures
import getpass
import html
import json
import posixpath
import shlex
import socket
import subprocess
import threading
import time

from nwebclient import nx
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


class HostDB:
    """
    Tabelle deployment_hosts der nweb-Persistence des JobRunners, ohne Persistence nur im Speicher.

    Spalten: host (Name), ssh (Ziel fuer ssh, leer = host), mobile, remote (per send/add_host eingetragen),
    info (JSON der Host-Infos), updated (Zeitstempel)
    """
    TABLE = 'deployment_hosts'
    COLUMNS = ('host', 'ssh', 'mobile', 'remote', 'info', 'updated')

    def __init__(self, owner: r.BaseJobExecutor):
        self.owner = owner
        self.table_created = False
        self.mem = {}
        self.lock = threading.RLock()

    def nweb(self):
        """ nweb-Persistence des JobRunners oder None, legt beim ersten Zugriff die Tabelle an """
        root = self.owner.getRoot()
        nw = getattr(root, 'nweb', None) if root is not None and root is not self.owner else None
        if nw is None or not hasattr(nw, 'dml') or not hasattr(nw, 'select'):
            return None
        if not self.table_created:
            try:
                nw.dml(f"""CREATE TABLE IF NOT EXISTS {self.TABLE} (
                    host VARCHAR(128) NOT NULL PRIMARY KEY,
                    ssh VARCHAR(255) DEFAULT NULL,
                    mobile INTEGER DEFAULT 0,
                    remote INTEGER DEFAULT 0,
                    info TEXT DEFAULT NULL,
                    updated INTEGER DEFAULT NULL
                )""")
                self.table_created = True
            except Exception as e:
                self.owner.error(f"Tabelle {self.TABLE}: {e}")
                return None
        return nw

    def available(self) -> bool:
        return self.nweb() is not None

    @classmethod
    def to_row(cls, values) -> dict:
        row = dict(zip(cls.COLUMNS, values))
        try:
            row['info'] = json.loads(row['info']) if row.get('info') else {}
        except ValueError:
            row['info'] = {}
        row['mobile'] = bool(row.get('mobile'))
        row['remote'] = bool(row.get('remote'))
        return row

    def all(self) -> list:
        nw = self.nweb()
        if nw is None:
            with self.lock:
                return [dict(row) for row in self.mem.values()]
        rows = nw.select(f"SELECT {', '.join(self.COLUMNS)} FROM {self.TABLE} ORDER BY host")
        return [self.to_row(row) for row in rows]

    def get(self, host) -> dict:
        nw = self.nweb()
        if nw is None:
            with self.lock:
                return dict(self.mem.get(host) or {})
        rows = nw.select(f"SELECT {', '.join(self.COLUMNS)} FROM {self.TABLE} WHERE host = ?", [host])
        return self.to_row(rows[0]) if rows else {}

    def save(self, host, **values):
        """ Legt den Host an oder aendert die uebergebenen Spalten (ssh, mobile, remote, info) """
        values['updated'] = int(time.time())
        with self.lock:
            nw = self.nweb()
            if nw is None:
                row = self.mem.setdefault(host, {'host': host, 'ssh': None, 'mobile': False, 'remote': False,
                                                 'info': {}, 'updated': None})
                row.update(values)
                return
            db_values = {k: (json.dumps(v) if k == 'info' else int(v) if isinstance(v, bool) else v)
                         for k, v in values.items()}
            cols = list(db_values)
            if nw.select(f"SELECT host FROM {self.TABLE} WHERE host = ?", [host]):
                nw.dml(f"UPDATE {self.TABLE} SET {', '.join(c + ' = ?' for c in cols)} WHERE host = ?",
                       [db_values[c] for c in cols] + [host])
            else:
                nw.dml(f"INSERT INTO {self.TABLE} (host, {', '.join(cols)}) VALUES ({', '.join(['?'] * (len(cols) + 1))})",
                       [host] + [db_values[c] for c in cols])

    def delete(self, host):
        with self.lock:
            nw = self.nweb()
            if nw is None:
                self.mem.pop(host, None)
            else:
                nw.dml(f"DELETE FROM {self.TABLE} WHERE host = ?", [host])


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

    def __init__(self, type='deployments', ssh='ssh', connect_timeout=5, timeout=60, update_timeout=600,
                 send_to=None):
        """
        :param send_to: URL einer anderen Instanz, bei deren Deployments sich dieser Rechner beim Start
                        als mobiler Host eintraegt (siehe send)
        """
        super().__init__(type)
        self.ssh_bin = ssh
        self.connect_timeout = int(connect_timeout)
        self.timeout = int(timeout)
        self.update_timeout = int(update_timeout)
        self.status_cache = {}
        self.last_output = ''
        self.last_cmd = ''
        self.hostdb = HostDB(self)
        self.send_to = send_to
        self.define_vars('connect_timeout', 'timeout', 'update_timeout')
        if send_to:
            self.delayed(2, lambda: self.send(send_to))

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
        """
        Hosts {name: {'host', 'ssh', 'mobile', 'remote'}}: SSH-Hosts der Deployments (mobil, wenn eines der
        Deployments mobil ist) und die per send/add_host eingetragenen Hosts der Tabelle
        """
        res = {}
        for dep in self.deployments():
            e = res.setdefault(dep.sshhost, {'host': dep.sshhost, 'ssh': dep.sshhost, 'mobile': False,
                                             'remote': False})
            e['mobile'] = e['mobile'] or dep.mobile
        try:
            rows = self.hostdb.all()
        except Exception as e:
            self.error(f"{HostDB.TABLE} lesen: {e}")
            rows = []
        for row in rows:
            if not row.get('remote'):
                continue
            e = res.setdefault(row['host'], {'host': row['host'], 'ssh': row['host'], 'mobile': False})
            e['ssh'] = row.get('ssh') or e['ssh']
            e['mobile'] = e['mobile'] or row.get('mobile', False)
            e['remote'] = True
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
        try:
            return self.hostdb.get(host).get('info') or {}
        except Exception as e:
            self.error(f"{HostDB.TABLE} lesen: {e}")
            return {}

    def save_hostinfo(self, host, info: dict):
        try:
            self.hostdb.save(host, info=info)
        except Exception as e:
            self.error(f"{HostDB.TABLE} schreiben: {e}")

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

    def query_host(self, host, ssh=None) -> dict:
        """
        Fragt die hostinfo-Werte von host ab, bei offline bleiben die alten Werte mit ihrem Zeitstempel

        :param ssh: Ziel fuer ssh, Standard ist der Eintrag des Hosts bzw. host
        """
        if ssh is None:
            ssh = (self.hosts().get(host) or {}).get('ssh') or host
        cmds = self.hostinfo_cmds()
        info = dict(self.get_hostinfo(host))
        info['checked'] = now = time.time()
        code, out = self.ssh(ssh, self.hostinfo_script(cmds)) if cmds else (0, '')
        if code in (255, 124):
            info.update(online=False, error=out.strip())
        else:
            values = dict(info.get('values') or {})
            for key, value in self.parse_hostinfo(out).items():
                values[key] = {'value': value, 'time': now}
            info.update(online=True, error='', last_online=now, values=values)
        self.save_hostinfo(host, info)
        return info

    def query_hosts(self, hosts: list) -> dict:
        entries = self.hosts()
        targets = [(entries.get(h) or {}).get('ssh') or h for h in hosts]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            return dict(zip(hosts, pool.map(self.query_host, hosts, targets)))

    # ------------------------------------------------------------------
    # Hosts senden / eintragen
    # ------------------------------------------------------------------

    @staticmethod
    def own_host_entry(host: str = '', ssh: str = '') -> dict:
        """ Eintrag dieses Rechners: Name = Hostname, ssh = <benutzer>@<eigene IP> """
        host = host or socket.gethostname()
        if not ssh:
            try:
                user = getpass.getuser()
            except Exception:
                user = ''
            ip = nx.get_ip()
            ssh = f"{user}@{ip}" if user else ip
        return {'host': host, 'ssh': ssh, 'mobile': 1}

    @op("Diesen Rechner bei der Deployments-Instanz unter url als mobilen Host eintragen",
        success={'host': 'dict'}, name='send')
    def send(self, url: str = '', host: str = '', ssh: str = '', remote_type: str = 'deployments'):
        """
        :param url: Adresse der anderen Instanz, z.B. http://server:7070/ (leer = send_to)
        :param host: Name des Hosts, Standard ist der Hostname
        :param ssh: Ziel fuer ssh, Standard ist <benutzer>@<eigene IP>
        :param remote_type: Name des Deployments-Runners auf der anderen Instanz
        """
        url = url or self.send_to
        if not url:
            return self.fail("keine url angegeben")
        entry = self.own_host_entry(host, ssh)
        res = r.remote(url, {'type': remote_type, 'op': 'add_host', **entry})
        if not isinstance(res, dict) or not res.get('success'):
            msg = (res.get('message') or res.get('error') or '') if isinstance(res, dict) else str(res)
            self.error(f"send an {url} fehlgeschlagen: {msg}")
            return self.fail(f"Senden an {url} fehlgeschlagen: {msg}", host=entry)
        self.info(f"Host {entry['host']} ({entry['ssh']}) an {url} gesendet")
        return self.success(f"{entry['host']} an {url} gesendet", host=entry)

    @op("Host eintragen bzw. aktualisieren (wird von send einer anderen Instanz aufgerufen)",
        success={'host': 'dict'}, name='add_host')
    def add_host(self, host: str = '', ssh: str = '', mobile: str = '1'):
        """
        :param host: Name des Hosts
        :param ssh: Ziel fuer ssh, z.B. pi@10.0.0.5 (leer = host)
        :param mobile: 1, wenn der Host nicht immer online ist
        """
        host = str(host or '').strip()
        if not host:
            return self.fail("host fehlt")
        self.hostdb.save(host, ssh=str(ssh or '').strip() or None, mobile=as_bool(mobile), remote=True)
        return self.success(f"Host {host} eingetragen", host=self.hostdb.get(host), ui_reload=True)

    @op("Per send/add_host eingetragenen Host entfernen", success={}, name='remove_host')
    def remove_host(self, host: str = ''):
        if not self.hostdb.get(host):
            return self.fail(f"Host {host} nicht gefunden")
        self.hostdb.delete(host)
        return self.success(f"Host {host} entfernt", ui_reload=True)

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

    def nav(self, p: base.Page, active=None):
        """ obere Navigation, active = Titel der aktuellen Seite """
        p.nav(("Übersicht", self.link(self.part_index), "Alle Deployments"),
              ("Hosts", self.link(self.part_hosts), "Infos je SSH-Host (Arg hostinfo)"),
              ("Letzte Ausgabe", self.link(self.part_output), "Ausgabe des letzten Updates"),
              active=active)

    def part_index(self, p: base.Page, params={}):
        self.nav(p, "Übersicht")
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
        res = esc(u.format_datetime(s.get('checked'), empty='-'))
        if s.get('checked') and not s.get('online'):
            res += f'<br><small title="Stand der angezeigten Version">online: {esc(u.format_datetime(s.get("last_online"), empty="-"))}</small>'
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
                                 ("Status", self.state(s)[1]), ("Geprüft", u.format_datetime(s.get('checked'), empty='-')),
                                 ("Zuletzt online", u.format_datetime(s.get('last_online'), empty='-')), ("Fehler", s.get('error'))):
                p.prop(title, esc(value or '-'))
            p.div(self.col_actions({'dep': dep}))
        self.part_last_update(p, s.get('last_update'))

    def part_last_update(self, p: base.Page, upd):
        if not upd:
            return
        with p.section(h=f"Letztes Update {u.format_datetime(upd.get('time'), empty='-')} ({'ok' if upd.get('ok') else 'Fehler'})"):
            p.pre(esc(upd.get('output') or ''))

    def part_output(self, p: base.Page, params={}):
        self.nav(p, "Letzte Ausgabe")
        with p.section(h="Letzte Ausgabe"):
            p.div(esc(self.last_cmd or "Seit dem Start wurde nichts aktualisiert."))
            p.pre(esc(self.last_output))

    # ------------------------------------------------------------------
    # UI: Hosts
    # ------------------------------------------------------------------

    def part_hosts(self, p: base.Page, params={}):
        self.nav(p, "Hosts")
        hosts = self.hosts()
        cmds = self.hostinfo_cmds()
        with p.section(h="Hosts"):
            if not hosts:
                p(w.alert_info(f"Keine Hosts, Deployments im Arg '{self.ARG_KEY}' eintragen oder von einer "
                               f"anderen Instanz senden (op send)."))
            else:
                if not cmds:
                    p(w.alert_info(f"Keine Befehle, im Arg '{self.HOSTINFO_KEY}' eintragen (schluessel: befehl)."))
                rows = [{**e, 'info': self.get_hostinfo(h)} for h, e in hosts.items()]
                p.grid(rows, self.host_cols(cmds), params, search=True)
                p.div(self.btn("Alle abfragen", 'hostinfo', "Fragt die Host-Infos aller Hosts per SSH ab"))
        with p.section(h="Diesen Host senden"):
            p.input('url', id='deployments_send_url', value=self.send_to or '')
            p(self.action_btn_parametric("Senden", {'type': self.type, 'op': 'send', 'url': '#deployments_send_url'}))
            p.pre('', id='result')

    def host_cols(self, cmds: dict):
        col = w.Grid.col
        cols = [col('host', "Host", html=True, value=self.col_host),
                col('mobile', "Mobil", value=lambda e: 'ja' if e['mobile'] else '')]
        for key, cmd in cmds.items():
            cols.append(col(key, key, html=True, value=lambda e, k=key: self.col_hostvalue(e, k),
                            sort=lambda e, k=key: self.hostvalue(e, k).get('value', ''),
                            td={'title': lambda e, c=cmd: c}))
        cols += [col('checked', "Geprüft", html=True, value=self.col_host_checked,
                     sort=lambda e: e['info'].get('checked') or 0),
                 col('actions', '', html=True, sortable=False, value=self.col_host_actions)]
        return cols

    @staticmethod
    def col_host(e):
        res = esc(e['host'])
        if e.get('ssh') and e['ssh'] != e['host']:
            res += f' <small>{esc(e["ssh"])}</small>'
        if e.get('remote'):
            res += ' <span class="badge tag" title="Per send von einem anderen Rechner eingetragen">gesendet</span>'
        return res

    def col_host_actions(self, e):
        res = self.btn("Abfragen", 'hostinfo', "Host-Infos per SSH abfragen", host=e['host'])
        if e.get('remote'):
            res += self.btn("Entfernen", 'remove_host', "Eingetragenen Host entfernen", host=e['host'])
        return res

    @staticmethod
    def hostvalue(e, key) -> dict:
        return (e['info'].get('values') or {}).get(key) or {}

    def col_hostvalue(self, e, key):
        entry = self.hostvalue(e, key)
        if not entry:
            return ''
        value = esc(entry.get('value', '')).replace('\n', '<br>')
        return f'<span title="Stand {esc(u.format_datetime(entry.get("time"), empty="-"))}">{value}</span>'

    @staticmethod
    def col_host_checked(e):
        info = e['info']
        res = esc(u.format_datetime(info.get('checked'), empty='-'))
        if info.get('checked') and not info.get('online'):
            res += (f' <span class="badge" title="{esc(info.get("error", ""))}">offline</span>'
                    f'<br><small title="Stand der angezeigten Werte">online: {esc(u.format_datetime(info.get("last_online"), empty="-"))}</small>')
        return res
