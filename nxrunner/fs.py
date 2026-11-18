import os
import shutil
import mimetypes
import urllib.parse

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d
from nwebclient.runner import op


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


class Browse(r.BaseJobExecutor):
    """
      Navigation durch das Dateisystem unterhalb von path (Verzeichnisse, Dateiinfo, Textvorschau, Download).
      Pfade ausserhalb von path sind nicht erreichbar.

      Config (runners):
          browse: nxrunner.fs:Browse(path="~")
          files: nxrunner.fs:Browse(path="/srv/data", type="files")

      Jobs:
          {"type": "browse", "op": "listdir", "dir": "sub/dir"}
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
        """ Eintraege direkt in current, Verzeichnisse zuerst """
        rows = []
        try:
            with os.scandir(current) as it:
                for e in it:
                    try:
                        is_dir = e.is_dir()
                        st = e.stat()
                    except OSError:
                        continue
                    rows.append({'name': e.name, 'dir': is_dir, 'path': e.path,
                                 'size': 0 if is_dir else st.st_size, 'mtime': st.st_mtime})
        except OSError as ex:
            self.error(f'Browse: {current}: {ex}')
        return sorted(rows, key=lambda r: (not r['dir'], r['name'].lower()))

    @op("Eintraege in dir (relativ zu path, nicht rekursiv)", result={'success': 'bool', 'entries': 'list'})
    def listdir(self, dir: str = ''):
        current = self.resolve_dir(dir)
        items = [{'name': e['name'], 'path': self.rel(e['path']), 'dir': e['dir'], 'size': e['size'],
                  'mtime': e['mtime']} for e in self.entries(current)]
        return self.success('ok', dir=self.rel(current), entries=items)

    # --- Seiten -------------------------------------------------------------------------------

    def part_breadcrumb(self, p: b.Page, rel: str):
        links = [w.a(w.s(os.path.basename(self.path) or self.path), self.link(self.part_index),
                     hint="Startverzeichnis: " + self.path)]
        parts = [x for x in rel.split('/') if x]
        for i, name in enumerate(parts):
            links.append(w.a(w.s(name), self.link(self.part_index, dir='/'.join(parts[:i + 1])),
                             hint="Verzeichnis öffnen"))
        p.div(' / '.join(links), style='margin: 8px 0; font-weight: 600;')

    def part_index(self, p: b.Page, params={}):
        current = self.resolve_dir(params.get('dir', ''))
        rel = self.rel(current)
        self.part_breadcrumb(p, rel)
        if rel != '':
            p.div(w.a(".. (übergeordnetes Verzeichnis)", self.link(self.part_index, dir=os.path.dirname(rel)),
                      hint="Übergeordnetes Verzeichnis öffnen"))
        p.grid(self.entries(current), [
            w.Grid.col('name', "Name", value=self.cell_name, html=True,
                       sort=lambda r: (not r['dir'], r['name'].lower())),
            w.Grid.col('size', "Größe", value=lambda r: '' if r['dir'] else u.format_size(r['size']),
                       sort=lambda r: r['size']),
            w.Grid.col('mtime', "Geändert", value=lambda r: u.format_date(r['mtime']), sort=lambda r: r['mtime']),
            w.Grid.col('actions', "", value=self.cell_actions, html=True, sortable=False)],
            params=params, page_size=100, search=lambda r: r['name'], empty="Verzeichnis ist leer",
            count="{shown} / {total} Einträge")

    def cell_name(self, row: dict) -> str:
        if row['dir']:
            return w.a(w.s(row['name'] + '/'), self.link(self.part_index, dir=self.rel(row['path'])),
                       hint="Verzeichnis öffnen")
        return w.a(w.s(row['name']), self.link(self.part_file, file=self.rel(row['path'])),
                   hint="Dateiinfo und Vorschau anzeigen")

    def cell_actions(self, row: dict) -> str:
        if row['dir']:
            return ''
        return w.a("Download", self.download_link(row['path']), hint="Datei herunterladen")

    def download_link(self, path: str) -> str:
        args = {'type': self.type, self.type: 'download', 'file': self.rel(path)}
        return '/pysys/dispatcher?' + urllib.parse.urlencode(args)  # absolut, auch in /pysys/ui

    def part_file(self, p: b.Page, params={}):
        path = self.resolve(params.get('file'))
        if path is None or not os.path.isfile(path):
            p.div("Datei nicht gefunden oder nicht erlaubt: " + w.s(params.get('file', '')), style='color: #c33;')
            return
        rel = self.rel(path)
        self.part_breadcrumb(p, os.path.dirname(rel))
        p.h2(w.s(os.path.basename(path)))
        st = os.stat(path)
        p.prop("Pfad", w.s(path))
        p.prop("Größe", u.format_size(st.st_size))
        p.prop("Geändert", u.format_date(st.st_mtime))
        p.prop("Typ", w.s(mimetypes.guess_type(path)[0] or 'unbekannt'))
        p.div(w.a("Download", self.download_link(path), hint="Datei herunterladen") + ' &nbsp; ' +
              w.a("Zurück", self.link(self.part_index, dir=os.path.dirname(rel)), hint="Zurück zum Verzeichnis"))
        self.part_preview(p, path)

    def part_preview(self, p: b.Page, path: str):
        """ Textvorschau der ersten PREVIEW_BYTES, Binaerdateien werden nicht angezeigt """
        try:
            with open(path, 'rb') as f:
                data = f.read(self.PREVIEW_BYTES)
        except OSError as ex:
            p.div("Nicht lesbar: " + w.s(str(ex)), style='color: #c33;')
            return
        if b'\0' in data:
            p.div("Binärdatei, keine Vorschau", style='opacity: .65;')
            return
        with p.section(h="Vorschau"):
            p.pre(w.s(data.decode('utf-8', errors='replace')))

    def page_download(self, params={}):
        from flask import send_file
        path = self.resolve(params.get('file'))
        if path is None or not os.path.isfile(path):
            return "404, Datei nicht gefunden oder nicht erlaubt", 404
        return send_file(path, as_attachment=True, download_name=os.path.basename(path))
