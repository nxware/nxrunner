
import functools
import io
import os
import re
import urllib.parse

from nwebclient import runner as r, base
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d
from nwebclient import NWebClient
from nwebclient import NWebGroup


IMAGE_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp')

LIGHTBOX_CSS = '''
.nwd-lightbox { position: fixed; inset: 0; z-index: 1000; background: rgba(0, 0, 0, .85); display: flex;
  flex-direction: column; align-items: center; justify-content: center; }
.nwd-lightbox[hidden] { display: none; }
.nwd-lightbox img { max-width: 92vw; max-height: 84vh; object-fit: contain; background: #222; }
.nwd-lightbox .nwd-bar { color: #eee; margin-top: 8px; font-size: 14px; display: flex; gap: 14px; align-items: center; }
.nwd-lightbox a { color: #9cf; }
.nwd-lightbox button { position: absolute; top: 50%; transform: translateY(-50%); font-size: 32px; background: none;
  border: none; color: #eee; cursor: pointer; padding: 10px 16px; }
.nwd-lightbox .nwd-prev { left: 8px; } .nwd-lightbox .nwd-next { right: 8px; }
.nwd-lightbox .nwd-close { top: 24px; right: 8px; transform: none; font-size: 26px; }
'''

# Lightbox fuer die Vorschaubilder (a.nwd-lb), nur einmal pro Dokument registriert (neue UI fuehrt Scripte erneut aus)
LIGHTBOX_JS = '''
(function() {
  if (window.nwdLightbox) return;
  var box = null, items = [], index = 0;
  function build() {
    box = document.createElement('div');
    box.className = 'nwd-lightbox'; box.hidden = true;
    box.innerHTML = '<button class="nwd-close" title="Schliessen (Esc)">&times;</button>'
      + '<button class="nwd-prev" title="Zurueck">&lsaquo;</button><img alt="">'
      + '<button class="nwd-next" title="Weiter">&rsaquo;</button>'
      + '<div class="nwd-bar"><span class="nwd-name"></span><span class="nwd-pos"></span>'
      + '<a class="nwd-orig" target="_blank">Original</a><a class="nwd-file">Datei</a></div>';
    document.body.appendChild(box);
    box.addEventListener('click', function(e) {
      if (e.target.closest('.nwd-prev')) show(index - 1);
      else if (e.target.closest('.nwd-next')) show(index + 1);
      else if (e.target === box || e.target.closest('.nwd-close')) close();
    });
  }
  function show(i) {
    if (!items.length) return;
    index = (i + items.length) % items.length;
    var a = items[index];
    box.querySelector('img').src = a.getAttribute('href');
    box.querySelector('.nwd-name').textContent = a.dataset.name;
    box.querySelector('.nwd-pos').textContent = (index + 1) + ' / ' + items.length;
    box.querySelector('.nwd-orig').href = a.getAttribute('href');
    box.querySelector('.nwd-file').href = a.dataset.file;
    box.hidden = false;
  }
  function close() { if (box) { box.hidden = true; box.querySelector('img').removeAttribute('src'); } }
  document.addEventListener('click', function(e) {
    var a = e.target.closest('a.nwd-lb');
    if (!a || e.ctrlKey || e.metaKey || e.shiftKey) return;
    e.preventDefault();
    if (!box || !document.body.contains(box)) build();
    items = Array.prototype.slice.call(document.querySelectorAll('a.nwd-lb'));
    show(items.indexOf(a));
  });
  document.addEventListener('keydown', function(e) {
    if (!box || box.hidden) return;
    if (e.key === 'Escape') close();
    else if (e.key === 'ArrowLeft') show(index - 1);
    else if (e.key === 'ArrowRight') show(index + 1);
  });
  window.nwdLightbox = true;
})();
'''


@functools.lru_cache(maxsize=1)
def is_wsl():
    try:
        with open('/proc/version') as f:
            return 'microsoft' in f.read().lower()
    except OSError:
        return False


def file_url(path) -> str:
    """
      Direkter file://-Link auf eine Datei, unter der WSL fuer Windows:
      /mnt/d/a/b.png -> file:///D:/a/b.png, /home/pi/b.png -> file://wsl.localhost/<Distribution>/home/pi/b.png
    """
    path = os.path.abspath(path)
    if is_wsl():
        parts = path.split('/')
        if len(parts) > 2 and parts[1] == 'mnt' and len(parts[2]) == 1 and parts[2].isalpha():
            return 'file:///' + parts[2].upper() + ':/' + urllib.parse.quote('/'.join(parts[3:]))
        distro = os.environ.get('WSL_DISTRO_NAME', 'Ubuntu')
        return 'file://wsl.localhost/' + urllib.parse.quote(distro) + urllib.parse.quote(path)
    return 'file://' + urllib.parse.quote(path)


class NWebDir(r.BaseJobExecutor):
    """
      Verzeichnis auf dem Server mit Auswahl einer nweb-Gruppe und eines ImageExecutors dieser Instanz.
      op=files                       Dateien im Verzeichnis (name, size, mtime)
      op=image_executors             geladene ImageExecutoren des Dispatchers (type, cls)
      op=set_group group_id=...      Gruppe festlegen (self.g), leer = keine Gruppe
      op=name_ids                    an Dateien mit passendem Dokument in self.g _id_<id> anhaengen (a.jpg -> a_id_42.jpg)
      Ist eine Gruppe festgelegt, zeigt die Dateiliste, ob es dort ein Dokument mit dem Dateinamen gibt
      (ohne Endung, siehe name_key). Dateien mit _id_<id> werden ueber die id zugeordnet (ok).
      Die Dateiliste zeigt die neuesten Dateien zuerst, PAGE_SIZE je Seite (Parameter pg), Bilder als Vorschau.
      Vorschaubilder ueber page=thumb (/pysys/runner?type=<type>&<type>=thumb&name=<datei>, mit Pillow verkleinert,
      sonst das Original). Klick auf ein Vorschaubild oeffnet es in einer Lightbox (Original ueber page=file,
      Pfeiltasten blaettern, Esc schliesst), der Dateiname ist ein file://-Link (unter der WSL auf den Windows-Pfad).
    """
    PAGE_SIZE = 50
    THUMB_SIZE = 160
    THUMB_CACHE = 500  # Anzahl Vorschaubilder im Speicher
    g: NWebGroup = None

    ID_RE = re.compile(r'^(.*)_id_(\d+)$')
    # nur genau die Zaehler _00001_ und _00002_ (wie bei ComfyUI bild_00001_.png) sind beim Namensvergleich egal,
    # andere Nummern bleiben verschieden (b_12345_ != b_12346_)
    COUNTER_RE = re.compile(r'_0000[12]_')

    def __init__(self, path, group=None, instance=None):
        super().__init__('nwebdir')
        self.define_vars('file_count')
        self.define_sig(d.PStr('op', 'files')).r(success='bool', message='str', value='list')
        self.define_sig(d.PStr('op', 'image_executors')).r(success='bool', message='str', value='list')
        self.define_sig(d.PStr('op', 'set_group'), d.PInt('group_id')).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'name_ids')).r(success='bool', message='str', renamed='list', skipped='list')
        self.c = NWebClient(instance)
        if group is not None:
            self.g = self.c.group(group)
        self.path = path
        self.thumbs = {}  # (name, mtime) -> JPEG

    def get_file_count(self):
        return len(list(self.files()))

    def files(self):
        """ Dateien direkt in self.path (ohne Unterverzeichnisse), neueste zuerst """
        res = []
        with os.scandir(self.path) as it:
            for e in it:
                if e.is_file():
                    st = e.stat()
                    res.append({'name': e.name, 'size': st.st_size, 'mtime': st.st_mtime})
        return sorted(res, key=lambda f: f['mtime'], reverse=True)

    def groups(self):
        return [{'id': g.id(), 'title': g.title()} for g in self.c.groups()]

    def image_executors(self):
        """ geladene Runner des Dispatchers, die Bilder verarbeiten (ImageExecutor) """
        instances = getattr(self.owner(), 'instances', None) or {}
        return [{'type': name, 'cls': type(runner).__name__} for name, runner in instances.items()
                if isinstance(runner, r.ImageExecutor)]

    def execute_files(self, data):
        return self.success('ok', value=self.files())

    def execute_image_executors(self, data):
        return self.success('ok', value=self.image_executors())

    def execute_set_group(self, data):
        group_id = str(data.get('group_id', '')).strip()
        if group_id == '':
            self.g = None
            return self.success('Keine Gruppe', ui_reload=True)
        try:
            # aus der Liste statt c.group(id): fuer unbekannte ids liefert die API keinen Fehler, sondern eine leere Liste
            group = next((g for g in self.c.groups() if str(g.id()) == group_id), None)
        except Exception as e:
            return self.fail(f'Gruppen nicht abrufbar: {e}')
        if group is None:
            return self.fail(f'Gruppe {group_id} nicht gefunden')
        self.g = group
        return self.success('Gruppe: ' + str(self.g.title()), ui_reload=True)

    @classmethod
    def split_id(cls, name):
        """ 'a_id_42.jpg' -> ('a', 42), 'a.jpg' -> ('a', None) """
        stem = os.path.splitext(str(name))[0]
        m = cls.ID_RE.match(stem)
        return (m.group(1), int(m.group(2))) if m else (stem, None)

    @classmethod
    def normalize(cls, stem) -> str:
        """ Zaehler _00001_/_00002_ entfernen: 'a_00001_' -> 'a', 'a_00002_b' -> 'a_b', 'a_12345_' bleibt """
        # am Ende ganz weg, in der Mitte bleibt ein _ als Trenner
        stem = re.sub(cls.COUNTER_RE.pattern + '$', '', stem)
        return cls.COUNTER_RE.sub('_', stem)

    @classmethod
    def name_key(cls, name) -> str:
        """
          Vergleichsschluessel ohne Dateiendung, _id_<id> und Zaehler _00001_/_00002_:
          a.jpg, a.png, a_id_42.jpg, a_00001_.png, a_00002_.jpg und a sind gleich, a_12345_ und a_12346_ nicht
        """
        return cls.normalize(cls.split_id(name)[0])

    def group_docs(self):
        """
          Dokumente der Gruppe self.g, ein Abruf fuer alle Dateien:
          ({name_key von Name und Titel: NWebDoc}, {document_id: NWebDoc})
        """
        by_key, by_id = {}, {}
        for doc in self.g.docs():
            by_id[int(doc.id())] = doc
            for key in (doc.get('name', None), doc.get('title', None)):
                if key:
                    by_key.setdefault(self.name_key(key), doc)
        return by_key, by_id

    def name_ids(self):
        """ _id_<id> an Dateien anhaengen, zu denen es in self.g ein Dokument gibt: {renamed: [(alt, neu)], skipped: [...]} """
        by_key, _ = self.group_docs()
        renamed, skipped = [], []
        for f in self.files():
            key, fid = self.split_id(f['name'])
            doc = by_key.get(self.normalize(key))  # key selbst bleibt fuer den neuen Namen unveraendert
            if fid is not None or doc is None:
                continue
            new = f"{key}_id_{doc.id()}{os.path.splitext(f['name'])[1]}"
            if os.path.exists(os.path.join(self.path, new)):
                skipped.append(f"{f['name']}: {new} existiert bereits")
                continue
            os.rename(os.path.join(self.path, f['name']), os.path.join(self.path, new))
            renamed.append((f['name'], new))
        return {'renamed': renamed, 'skipped': skipped}

    def execute_name_ids(self, data):
        if self.g is None:
            return self.fail('Keine Gruppe festgelegt')
        try:
            res = self.name_ids()
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')
        msg = f"{len(res['renamed'])} Dateien umbenannt" + (f", {len(res['skipped'])} uebersprungen" if res['skipped'] else '')
        return self.success(msg, ui_reload=True, **res)

    def part_index(self, p: base.Page, params={}):
        with p.section(h="Auswahl"):
            try:
                p.form_select('group_id', "Gruppe", {'': '(keine)', **{g['id']: g['title'] for g in self.groups()}},
                              self.g.id() if self.g is not None else params.get('group_id'), id='group_id')
                p(self.action_btn_parametric("Gruppe festlegen", {'type': self.type, 'op': 'set_group',
                                                                  'group_id': '#group_id'}))
                if self.g is not None:
                    p(self.action_btn_parametric("Name IDs", {'type': self.type, 'op': 'name_ids'}))
            except Exception as e:
                p.alert_error("Gruppen nicht abrufbar: " + w.s(e))
            executors = self.image_executors()
            if executors:
                p.form_select('executor', "ImageExecutor", {e['type']: f"{e['type']} ({e['cls']})" for e in executors},
                              params.get('executor'), id='executor')
            else:
                p.alert_info("Keine ImageExecutoren geladen")
        with p.section(h="Dateien in " + self.path):
            try:
                files = self.files()
            except OSError as e:
                p.alert_error(w.s(e))
            else:
                docs = None
                if self.g is not None:
                    try:
                        by_key, by_id = docs = self.group_docs()
                    except Exception as e:
                        p.alert_error("Dokumente der Gruppe nicht abrufbar: " + w.s(e))
                pages = max(1, -(-len(files) // self.PAGE_SIZE))
                try:
                    pg = min(max(1, int(params.get('pg', 1))), pages)
                except (TypeError, ValueError):
                    pg = 1
                nav = self.html_pagination(pg, pages)
                p(nav)
                titles = ['', 'Name', 'Größe', 'Alter'] + (['In Gruppe ' + w.s(self.g.title())] if docs is not None else [])
                head = w.tr(''.join(w.tag('th', t) for t in titles))
                rows = ''
                for f in files[(pg - 1) * self.PAGE_SIZE:pg * self.PAGE_SIZE]:
                    url = w.s(file_url(os.path.join(self.path, f['name'])))
                    preview = ''
                    if f['name'].lower().endswith(IMAGE_EXTENSIONS):
                        thumb = w.s(self.thumb_url(f['name']))
                        full = w.s(self.file_route(f['name']))
                        preview = (f'<a href="{full}" class="nwd-lb" data-name="{w.s(f["name"])}" data-file="{url}">'
                                   f'<img src="{thumb}" loading="lazy" alt="" '
                                   f'style="max-width: {self.THUMB_SIZE}px; max-height: {self.THUMB_SIZE}px;"></a>')
                    cells = [preview, f'<a href="{url}">{w.s(f["name"])}</a>', u.format_size(f['size']),
                             u.format_age(f['mtime'])]
                    if docs is not None:
                        key, fid = self.split_id(f['name'])
                        if fid is not None:
                            # per Name IDs zugeordnet
                            doc = by_id.get(fid)
                            cells.append(w.a(f'ok ({fid})', doc.url()) if doc is not None else f'id {fid} nicht in Gruppe')
                        else:
                            doc = by_key.get(self.normalize(key))
                            cells.append(w.a('ja (' + str(doc.id()) + ')', doc.url()) if doc is not None else 'nein')
                    rows += w.tr(cells)
                p(w.tag('table', head + rows))
                p(nav)
                p.div(f"{len(files)} Dateien")
                p(w.tag('style', LIGHTBOX_CSS))
                p.script(LIGHTBOX_JS)

    def thumb_url(self, name) -> str:
        return '/pysys/runner?' + urllib.parse.urlencode({'type': self.type, self.type: 'thumb', 'name': name})

    def thumbnail(self, name):
        """ Vorschaubild als JPEG (bytes), None ohne Pillow oder wenn das Bild nicht lesbar ist """
        path = os.path.join(self.path, name)
        key = (name, os.stat(path).st_mtime)
        if key in self.thumbs:
            return self.thumbs[key]
        try:
            from PIL import Image
            with Image.open(path) as img:
                img.thumbnail((self.THUMB_SIZE, self.THUMB_SIZE))
                out = io.BytesIO()
                img.convert('RGB').save(out, 'JPEG', quality=80)
        except Exception:
            return None
        if len(self.thumbs) >= self.THUMB_CACHE:
            self.thumbs.pop(next(iter(self.thumbs)))  # aeltesten Eintrag verwerfen
        self.thumbs[key] = out.getvalue()
        return self.thumbs[key]

    def file_route(self, name) -> str:
        return '/pysys/runner?' + urllib.parse.urlencode({'type': self.type, self.type: 'file', 'name': name})

    def local_name(self, params):
        """ name=<datei> aus den Parametern, nur Dateien direkt in self.path, sonst None """
        name = str(params.get('name', ''))
        if name != os.path.basename(name) or name in ('', '.', '..') or not os.path.isfile(os.path.join(self.path, name)):
            return None
        return name

    def page_file(self, params={}):
        """ Originaldatei direkt aus self.path (name=<datei>), z.B. fuer die Lightbox """
        import flask
        name = self.local_name(params)
        if name is None:
            return flask.Response('Datei nicht gefunden', status=404)
        return flask.send_from_directory(os.path.abspath(self.path), name, max_age=3600)

    def page_thumb(self, params={}):
        """ Vorschaubild einer Datei direkt in self.path (name=<datei>) """
        import flask
        name = self.local_name(params)
        if name is None:
            return flask.Response('Datei nicht gefunden', status=404)
        data = self.thumbnail(name)
        if data is None:
            # ohne Pillow bzw. nicht lesbar: Original ausliefern, der Browser verkleinert per CSS
            return flask.send_from_directory(os.path.abspath(self.path), name, max_age=3600)
        return flask.Response(data, mimetype='image/jpeg', headers={'Cache-Control': 'max-age=3600'})

    def html_pagination(self, pg, pages) -> str:
        """ Links ?type=..&<type>=index&pg=n (funktionieren in /pysys/runner und /pysys/ui) """
        def link(title, n):
            q = urllib.parse.urlencode({'type': self.type, self.type: 'index', 'pg': n})
            return w.a(title, '?' + q)
        parts = [link('« Zurück', pg - 1) if pg > 1 else '« Zurück', f'Seite {pg} von {pages}',
                 link('Weiter »', pg + 1) if pg < pages else 'Weiter »']
        return w.tag('div', ' | '.join(parts), style='margin: 6px 0;')
