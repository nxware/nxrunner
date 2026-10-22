
import os
import re

from nwebclient import runner as r, base
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d
from nwebclient import NWebClient
from nwebclient import NWebGroup


class NWebDir(r.BaseJobExecutor):
    """
      Verzeichnis auf dem Server mit Auswahl einer nweb-Gruppe und eines ImageExecutors dieser Instanz.
      op=files                       Dateien im Verzeichnis (name, size, mtime)
      op=image_executors             geladene ImageExecutoren des Dispatchers (type, cls)
      op=set_group group_id=...      Gruppe festlegen (self.g), leer = keine Gruppe
      op=name_ids                    an Dateien mit passendem Dokument in self.g _id_<id> anhaengen (a.jpg -> a_id_42.jpg)
      Ist eine Gruppe festgelegt, zeigt die Dateiliste, ob es dort ein Dokument mit dem Dateinamen gibt
      (ohne Endung, siehe name_key). Dateien mit _id_<id> werden ueber die id zugeordnet (ok).
    """
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

    def get_file_count(self):
        return len(list(self.files()))

    def files(self):
        """ Dateien direkt in self.path (ohne Unterverzeichnisse), nach Name sortiert """
        res = []
        with os.scandir(self.path) as it:
            for e in it:
                if e.is_file():
                    st = e.stat()
                    res.append({'name': e.name, 'size': st.st_size, 'mtime': st.st_mtime})
        return sorted(res, key=lambda f: f['name'].lower())

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
                titles = ['Name', 'Größe', 'Alter'] + (['In Gruppe ' + w.s(self.g.title())] if docs is not None else [])
                head = w.tr(''.join(w.tag('th', t) for t in titles))
                rows = ''
                for f in files:
                    cells = [w.s(f['name']), u.format_size(f['size']), u.format_age(f['mtime'])]
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
                p.div(f"{len(files)} Dateien")
