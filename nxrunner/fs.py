import json
import os
import shutil
import time
import mimetypes
import urllib.parse

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d
from nwebclient.runner import op

from nxrunner.jobstore import RemoteJobMixin


class Smb(r.BaseJobExecutor):
    """
      Zugriff auf SMB-/Windows-Freigaben (smbprotocol), z.B. als Backup-Ziel.
      Pfade sind /share/verzeichnis/datei, vorher connect(host, username, password) aufrufen.
      Methoden: listdir, get_contents, put_contents, makedirs, copy_to_remote
    """

    MODULES = ['smbprotocol']

    def __init__(self):
        super(Smb, self).__init__('smb')

    def connect(self, host, username, password):
        import smbclient
        self.client = smbclient
        self.host = host
        smbclient.ClientConfig(username=username, password=password)

    def _build_path(self, path):
        path = path.replace('/', '\\')
        return "\\\\" + self.host + path

    def listdir(self, path='/'):
        return self.client.listdir(self._build_path(path))

    def get_contents(self, path="/"):
        with self.client.open_file(self._build_path(path), mode="r") as fd:
            return fd.read()

    def put_contents(self, path="/", contents=None):
        with self.client.open_file(self._build_path(path), mode="w") as fd:
            fd.write(contents)

    def makedirs(self, path="/"):
        self.client.makedirs(self._build_path(path), exist_ok=True)

    def copy_to_remote(self, src_path="/", dst_path="/", mtime=None):
        """
          Kopiert die lokale Datei src_path nach dst_path (/share/verzeichnis/datei).
          Das Zielverzeichnis muss existieren (siehe makedirs).
        """
        with open(src_path, 'rb') as src, self.client.open_file(self._build_path(dst_path), mode='wb') as dst:
            shutil.copyfileobj(src, dst, 1024 * 1024)
        if mtime is not None:
            self.client.utime(self._build_path(dst_path), times=(mtime, mtime))


class BrowseView:
    """
      Oberflaeche fuer Browse und BrowseRemote: Pfadleiste, Verzeichnisliste (p.grid) und Dateiansicht.
      Zeilen (Browse.listdir) und Dateiinfos (Browse.fileinfo) haben Pfade relativ zum Startverzeichnis.
      Unterklassen setzen root_label(), root_hint() und download_link(rel) (None = kein Download) um.
    """

    def root_label(self) -> str:
        return '/'

    def root_hint(self) -> str:
        return "Startverzeichnis"

    def download_link(self, rel: str):
        return None

    @staticmethod
    def norm(rel) -> str:
        """ relativer Pfad mit / ohne fuehrende und doppelte Trenner """
        return '/'.join(x for x in str(rel or '').replace('\\', '/').split('/') if x and x != '.')

    @staticmethod
    def parent(rel: str) -> str:
        return rel.rpartition('/')[0]

    def part_breadcrumb(self, p: b.Page, rel: str):
        links = [w.a(w.s(self.root_label()), self.link(self.part_index), hint=self.root_hint())]
        parts = [x for x in rel.split('/') if x]
        for i, name in enumerate(parts):
            links.append(w.a(w.s(name), self.link(self.part_index, dir='/'.join(parts[:i + 1])),
                             hint="Verzeichnis öffnen"))
        p.div(' / '.join(links), style='margin: 8px 0; font-weight: 600;')

    def part_parent(self, p: b.Page, rel: str):
        if rel != '':
            p.div(w.a(".. (übergeordnetes Verzeichnis)", self.link(self.part_index, dir=self.parent(rel)),
                      hint="Übergeordnetes Verzeichnis öffnen"))

    def part_listing(self, p: b.Page, rows: list, params={}):
        p.grid(rows, [
            w.Grid.col('name', "Name", value=self.cell_name, html=True,
                       sort=lambda r: (not r['dir'], r['name'].lower())),
            w.Grid.col('size', "Größe", value=lambda r: '' if r['dir'] else u.format_size(r['size']),
                       sort=lambda r: r['size'] or 0),
            w.Grid.col('mtime', "Geändert", value=lambda r: u.format_date(r['mtime']), sort=lambda r: r['mtime'] or 0),
            w.Grid.col('actions', "", value=self.cell_actions, html=True, sortable=False)],
            params=params, page_size=100, search=lambda r: r['name'], empty="Verzeichnis ist leer",
            count="{shown} / {total} Einträge")

    def cell_name(self, row: dict) -> str:
        if row['dir']:
            return w.a(w.s(row['name'] + '/'), self.link(self.part_index, dir=row['path']), hint="Verzeichnis öffnen")
        return w.a(w.s(row['name']), self.link(self.part_file, file=row['path']),
                   hint="Dateiinfo und Vorschau anzeigen")

    def cell_actions(self, row: dict) -> str:
        link = None if row['dir'] else self.download_link(row['path'])
        return w.a("Download", link, hint="Datei herunterladen") if link else ''

    def part_fileinfo(self, p: b.Page, info: dict):
        """ info aus Browse.fileinfo """
        rel = info.get('path', '')
        p.h2(w.s(info.get('name') or rel))
        p.prop("Pfad", w.s(info.get('abspath') or rel))
        p.prop("Größe", u.format_size(info.get('size')))
        p.prop("Geändert", u.format_date(info.get('mtime')))
        p.prop("Typ", w.s(info.get('mime') or 'unbekannt'))
        links = []
        download = self.download_link(rel)
        if download:
            links.append(w.a("Download", download, hint="Datei herunterladen"))
        links.append(w.a("Zurück", self.link(self.part_index, dir=self.parent(rel)), hint="Zurück zum Verzeichnis"))
        p.div(' &nbsp; '.join(links))
        if info.get('error'):
            p.div("Nicht lesbar: " + w.s(info['error']), style='color: #c33;')
        elif info.get('binary'):
            p.div("Binärdatei, keine Vorschau", style='opacity: .65;')
        elif info.get('preview') is not None:
            with p.section(h="Vorschau"):
                p.pre(w.s(info['preview']))


class Browse(r.BaseJobExecutor, BrowseView):
    """
      Navigation durch das Dateisystem unterhalb von path (Verzeichnisse, Dateiinfo, Textvorschau, Download).
      Pfade ausserhalb von path sind nicht erreichbar. Ziel-Runner fuer BrowseRemote.

      Config (runners):
          browse: nxrunner.fs:Browse(path="~")
          files: nxrunner.fs:Browse(path="/srv/data", type="files")

      Jobs:
          {"type": "browse", "op": "listdir", "dir": "sub/dir"}
          {"type": "browse", "op": "fileinfo", "file": "sub/dir/a.txt"}
    """

    PREVIEW_BYTES = 64 * 1024

    def __init__(self, path: str = '~', type: str = 'browse'):
        super().__init__(type)
        self.path = os.path.realpath(os.path.expanduser(path))

    # --- Pfade --------------------------------------------------------------------------------

    def resolve(self, rel):
        """ :return: existierender absoluter Pfad innerhalb von path oder None """
        path = os.path.realpath(os.path.join(self.path, str(rel or '').lstrip('/\\')))
        if path == self.path or path.startswith(self.path.rstrip(os.sep) + os.sep):
            return path if os.path.exists(path) else None
        return None

    def resolve_dir(self, rel) -> str:
        path = self.resolve(rel)
        return path if path is not None and os.path.isdir(path) else self.path

    def rel(self, path: str) -> str:
        rel = os.path.relpath(path, self.path).replace('\\', '/')
        return '' if rel == '.' else rel

    def entries(self, current: str) -> list:
        """ Eintraege direkt in current (Pfade relativ zu path), Verzeichnisse zuerst """
        rows = []
        try:
            with os.scandir(current) as it:
                for e in it:
                    try:
                        is_dir = e.is_dir()
                        st = e.stat()
                    except OSError:
                        continue
                    rows.append({'name': e.name, 'dir': is_dir, 'path': self.rel(e.path),
                                 'size': 0 if is_dir else st.st_size, 'mtime': st.st_mtime})
        except OSError as ex:
            self.error(f'Browse: {current}: {ex}')
        return sorted(rows, key=lambda r: (not r['dir'], r['name'].lower()))

    def read_preview(self, path: str):
        """ :return: (text oder None, binary, fehler) der ersten PREVIEW_BYTES """
        try:
            with open(path, 'rb') as f:
                data = f.read(self.PREVIEW_BYTES)
        except OSError as ex:
            return None, False, str(ex)
        if b'\0' in data:
            return None, True, ''
        return data.decode('utf-8', errors='replace'), False, ''

    @op("Eintraege in dir (relativ zu path, nicht rekursiv)", result={'success': 'bool', 'dir': 'str', 'entries': 'list'})
    def listdir(self, dir: str = ''):
        current = self.resolve_dir(dir)
        return self.success('ok', dir=self.rel(current), entries=self.entries(current))

    @op("Info und Textvorschau einer Datei (relativ zu path)", result={'success': 'bool', 'message': 'str', 'info': 'dict'})
    def fileinfo(self, file: str = ''):
        path = self.resolve(file)
        if path is None or not os.path.isfile(path):
            return self.fail("Datei nicht gefunden oder nicht erlaubt: " + str(file))
        st = os.stat(path)
        preview, binary, error = self.read_preview(path)
        return self.success('ok', info={'path': self.rel(path), 'name': os.path.basename(path), 'abspath': path,
                                        'size': st.st_size, 'mtime': st.st_mtime,
                                        'mime': mimetypes.guess_type(path)[0], 'binary': binary,
                                        'preview': preview, 'error': error})

    # --- Seiten -------------------------------------------------------------------------------

    def root_label(self) -> str:
        return os.path.basename(self.path) or self.path

    def root_hint(self) -> str:
        return "Startverzeichnis: " + self.path

    def download_link(self, rel: str):
        args = {'type': self.type, self.type: 'download', 'file': rel}
        return '/pysys/dispatcher?' + urllib.parse.urlencode(args)  # absolut, auch in /pysys/ui

    def part_index(self, p: b.Page, params={}):
        res = self.listdir(params.get('dir', ''))
        self.part_breadcrumb(p, res['dir'])
        self.part_parent(p, res['dir'])
        self.part_listing(p, res['entries'], params)

    def part_file(self, p: b.Page, params={}):
        res = self.fileinfo(params.get('file', ''))
        if not res['success']:
            p.div(w.s(res['message']), style='color: #c33;')
            return
        self.part_breadcrumb(p, self.parent(res['info']['path']))
        self.part_fileinfo(p, res['info'])

    def page_download(self, params={}):
        from flask import send_file
        path = self.resolve(params.get('file'))
        if path is None or not os.path.isfile(path):
            return "404, Datei nicht gefunden oder nicht erlaubt", 404
        return send_file(path, as_attachment=True, download_name=os.path.basename(path))


class BrowseRemote(r.BaseJobExecutor, RemoteJobMixin, BrowseView):
    """
      Browse einer anderen Instanz ueber einen Job-Speicher (wie RemoteRunner): Verzeichnisse (listdir) und
      Dateiinfos (fileinfo) werden als Job an den Browse-Runner remote_type auf target gesendet.

      Jede Anfrage wird mit guid, Status und Ergebnis in der Tabelle browse_remote_job der nweb-Persistence
      (root.nweb) gespeichert: Nach einem Neustart werden offene guids weiter abgefragt (cresult), fertige
      Ergebnisse werden angezeigt, bis "Aktualisieren" eine neue Anfrage sendet (aeltere abgeschlossene
      Anfragen zu demselben Pfad werden dann entfernt). Ohne Persistence bleibt alles nur im Speicher.
      Downloads sind remote nicht moeglich.

      Config (runners):
          rbrowse: nxrunner.fs:BrowseRemote(target="pi4")
          rbrowse: nxrunner.fs:BrowseRemote(target="pi4", store="nweb_jobs", remote_type="files")

      Auf target: browse: nxrunner.fs:Browse(path="~")

      Jobs:
          {"type": "rbrowse", "op": "refresh", "what": "listdir", "arg": "sub/dir"}
          {"type": "rbrowse", "op": "requests"}
    """

    TAGS = ['remote']
    TABLE = 'browse_remote_job'
    REMOTE_ARGS = {'listdir': 'dir', 'fileinfo': 'file'}

    def __init__(self, type='browse_remote', target='', store='mongodb', remote_type='browse', poll_interval=10,
                 timeout=6000):
        """
        :param target: Name der Ziel-Instanz (nwebclient.nx.get_name() dort, i.d.R. der Hostname)
        :param store: type des Job-Speicher-Runners (JobStoreMixin)
        :param remote_type: type des Browse-Runners auf der Ziel-Instanz
        :param poll_interval: Sekunden zwischen den Abfragen der Ergebnisse
        :param timeout: Sekunden, nach denen eine Antwort verworfen wird
        """
        super().__init__(type)
        self.init_remote(target, store, remote_type, poll_interval, timeout)
        self.requests = {}  # (op, arg) -> letzte Anfrage {guid, op, arg, status, sent, done, result}
        self.done = {}      # (op, arg) -> letzte erfolgreiche Anfrage (bleibt beim Aktualisieren sichtbar)
        self.loaded = False
        self.table_created = False
        self.define_vars('target', 'store', 'remote_type')

    def set_target(self, value):
        """ setvar target: Anfragen des neuen Ziels laden """
        with self.remote_lock:
            self.target = str(value or '')
            self.requests, self.done, self.remote_pending = {}, {}, {}
            self.remote_error = ''
            self.loaded = False

    # --- nweb-Persistence (Tabelle browse_remote_job) ----------------------------------------

    def nweb(self):
        """ nweb-Persistence oder None (dann nur im Speicher) """
        root = self.getRoot()
        nw = getattr(root, 'nweb', None) if root is not None else None
        if nw is None or not hasattr(nw, 'dml'):
            return None
        if not self.table_created:
            self.create_table(nw)
        return nw

    def create_table(self, nw):
        nw.dml(f"""CREATE TABLE IF NOT EXISTS {self.TABLE} (
            guid varchar(64) NOT NULL PRIMARY KEY,
            runner varchar(255) NOT NULL,
            target varchar(255) NOT NULL,
            op varchar(32) NOT NULL,
            arg varchar(1024) NOT NULL,
            status varchar(16) NOT NULL,
            sent INTEGER DEFAULT NULL,
            done INTEGER DEFAULT NULL,
            result TEXT
        )""")
        nw.dml(f"CREATE INDEX IF NOT EXISTS {self.TABLE}_rt ON {self.TABLE} (runner, target)")
        self.table_created = True

    def save_entry(self, e: dict):
        """ unter remote_lock: Anfrage speichern, bei Abschluss aeltere abgeschlossene zu (op, arg) entfernen """
        try:
            nw = self.nweb()
            if nw is None:
                return
            result = None if e['result'] is None else json.dumps(e['result'], default=str)
            nw.dml(f"DELETE FROM {self.TABLE} WHERE guid = ?", [e['guid']])
            nw.dml(f"INSERT INTO {self.TABLE} (guid, runner, target, op, arg, status, sent, done, result) "
                   "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   [e['guid'], self.type, self.target, e['op'], e['arg'], e['status'],
                    int(e['sent']) if e['sent'] else None, int(e['done']) if e['done'] else None, result])
            if e['status'] == 'done':
                nw.dml(f"DELETE FROM {self.TABLE} WHERE runner = ? AND target = ? AND op = ? AND arg = ? "
                       "AND guid <> ? AND status <> 'pending'", [self.type, self.target, e['op'], e['arg'], e['guid']])
        except Exception as ex:
            self.error(f"Anfrage {e['guid']} nicht gespeichert: {ex}")

    def ensure_loaded(self):
        """ unter remote_lock: gespeicherte Anfragen einmalig laden, offene guids wieder abfragen """
        if self.loaded:
            return
        self.loaded = True
        try:
            nw = self.nweb()
            if nw is None:
                return
            rows = nw.select(f"SELECT guid, op, arg, status, sent, done, result FROM {self.TABLE} "
                             "WHERE runner = ? AND target = ? ORDER BY sent", [self.type, self.target])
        except Exception as ex:
            self.error("Gespeicherte Anfragen nicht geladen: " + str(ex))
            return
        for row in rows:
            self.restore_entry(row)
        if self.remote_pending:
            self.start_remote_poller()

    def restore_entry(self, row):
        """ unter remote_lock: Zeile der Tabelle uebernehmen, offene guid in remote_pending """
        guid, op_name, arg, status, sent, done, result = row
        try:
            result = json.loads(result) if result else None
        except ValueError:
            result = None
        e = dict(guid=guid, op=op_name, arg=arg or '', status=status, sent=sent, done=done, result=result)
        key = (op_name, e['arg'])
        self.requests[key] = e
        if status == 'done':
            self.done[key] = e
        elif status == 'pending' and guid not in self.remote_pending:
            self.remote_seq += 1
            self.remote_pending[guid] = dict(guid=guid, op=op_name, arg=e['arg'], seq=self.remote_seq,
                                             ts=sent or time.time(), label=f'{op_name} {e["arg"]}'.strip())

    # --- RemoteJobMixin -----------------------------------------------------------------------

    def remote_job_label(self, job: dict):
        return f"{job.get('op', '')} {job.get(self.REMOTE_ARGS.get(job.get('op'), ''), '')}".strip()

    def on_remote_result(self, meta: dict, result: dict):
        key = (meta.get('op'), meta.get('arg', ''))
        e = self.requests.get(key)
        if e is None or e['guid'] != meta['guid']:
            e = dict(guid=meta['guid'], op=key[0], arg=key[1], sent=meta.get('ts'))
            if key not in self.requests:
                self.requests[key] = e
        e.update(status='done' if result.get('success', False) else 'error', done=time.time(), result=result)
        if e['status'] == 'done':
            self.done[key] = e
        self.save_entry(e)

    def remote_changed(self):
        """ offene Anfragen, die nicht mehr abgefragt werden (Timeout), als timeout speichern """
        for e in self.requests.values():
            if e['status'] == 'pending' and e['guid'] not in self.remote_pending:
                e.update(status='timeout', done=time.time())
                self.save_entry(e)

    # --- Anfragen -----------------------------------------------------------------------------

    def request(self, op_name: str, arg: str = '', force: bool = False) -> dict:
        """
          Letzte Anfrage (op, arg), sendet eine neue, wenn es keine gibt oder force (ausser sie ist noch offen).
          Fehler beim Senden als Exception.
        """
        key = (op_name, self.norm(arg))
        with self.remote_lock:
            self.ensure_loaded()
            e = self.requests.get(key)
            if e is not None and (not force or e['status'] == 'pending'):
                return dict(e)
        guid = self.remote_send({'op': op_name, self.REMOTE_ARGS[op_name]: key[1]}, op=op_name, arg=key[1])
        with self.remote_lock:
            e = dict(guid=guid, op=op_name, arg=key[1], status='pending', sent=time.time(), done=None, result=None)
            self.requests[key] = e
            self.save_entry(e)
            return dict(e)

    def last_result(self, op_name: str, arg: str):
        """ :return: Ergebnis der letzten erfolgreichen Anfrage (op, arg) oder None """
        with self.remote_lock:
            e = self.done.get((op_name, self.norm(arg)))
            return e['result'] if e is not None else None

    @op("Anfrage (what=listdir mit Verzeichnis, what=fileinfo mit Datei als arg) erneut an die Ziel-Instanz senden",
        result={'success': 'bool', 'message': 'str', 'guid': 'str'})
    def refresh(self, what: str = 'listdir', arg: str = ''):
        if what not in self.REMOTE_ARGS:
            return self.fail("what muss listdir oder fileinfo sein")
        try:
            e = self.request(what, arg, force=True)
        except Exception as ex:
            return self.fail(str(ex))
        return self.success('gesendet', guid=e['guid'], ui_reload=True)

    @op("Gespeicherte Anfragen (ohne Ergebnis)", name='requests', result={'success': 'bool', 'requests': 'list'})
    def requests_list(self):
        with self.remote_lock:
            self.ensure_loaded()
            items = [{k: v for k, v in e.items() if k != 'result'} for e in self.requests.values()]
        return self.success('ok', requests=sorted(items, key=lambda e: e.get('sent') or 0, reverse=True))

    # --- Seiten -------------------------------------------------------------------------------

    def root_label(self) -> str:
        return self.target or '(kein target)'

    def root_hint(self) -> str:
        return f"Startverzeichnis des Runners {self.remote_type} auf {self.target}"

    def reload_later(self, p: b.Page):
        """ Seite nach poll_interval (mind. 2 s) neu laden, solange die Antwort aussteht """
        p.js_ready(f'setTimeout(function() {{ window.location.reload(); }}, {int(max(self.poll_interval, 2) * 1000)});')

    def part_header(self, p: b.Page):
        if not self.target:
            p(w.alert_error("target (Name der Ziel-Instanz) ist nicht gesetzt"))
        with self.remote_lock:
            self.ensure_loaded()
            p.prop("Status", w.s(self.remote_status()))
        stored = self.nweb() is not None
        p.prop("Anfragen gespeichert", f"ja (nweb-Persistence, Tabelle {self.TABLE})" if stored else "nein, nur im Speicher")

    def part_request(self, p: b.Page, op_name: str, arg: str):
        """ Stand der Anfrage (op, arg), sendet sie beim ersten Aufruf; laedt neu, solange sie offen ist """
        try:
            e = self.request(op_name, arg)
        except Exception as ex:
            p(w.alert_error(w.s(str(ex))))
            return
        btn = self.action_btn(dict(type=self.type, op='refresh', what=op_name, arg=e['arg']), label="Aktualisieren",
                              hint="Erneut bei der Ziel-Instanz anfragen")
        sent = f"Anfrage {w.s(e['guid'])} vom {u.format_date(e['sent'])}"
        if e['status'] == 'pending':
            p(w.alert_info(f"{sent} wurde von {w.s(self.target)} noch nicht beantwortet, die Seite prüft erneut."))
            self.reload_later(p)
        elif e['status'] == 'timeout':
            p(w.alert_error(f"{sent}: keine Antwort von {w.s(self.target)} nach {int(self.timeout)} s."))
            p.div(btn)
        elif e['status'] == 'error':
            message = (e['result'] or {}).get('message', 'Fehler auf ' + self.target)
            p(w.alert_error(f"{sent}: {w.s(str(message))}"))
            p.div(btn)
        else:
            p.div(f"Stand {u.format_date(e['done'])} &nbsp; " + btn, style='margin: 6px 0;')

    def part_index(self, p: b.Page, params={}):
        rel = self.norm(params.get('dir', ''))
        self.part_header(p)
        self.part_breadcrumb(p, rel)
        self.part_parent(p, rel)
        self.part_request(p, 'listdir', rel)
        result = self.last_result('listdir', rel)
        if result is not None:
            self.part_listing(p, result.get('entries') or [], params)

    def part_file(self, p: b.Page, params={}):
        rel = self.norm(params.get('file', ''))
        self.part_header(p)
        self.part_breadcrumb(p, self.parent(rel))
        self.part_request(p, 'fileinfo', rel)
        result = self.last_result('fileinfo', rel)
        if result is not None:
            self.part_fileinfo(p, result.get('info') or {})
