import fnmatch
import os


class Source:
    """
      Quelle eines Backups. Liefert die zu sichernden Dateien als (relativer Pfad, absoluter Pfad, stat).
      Neue Arten: Unterklasse mit KIND anlegen und in SOURCES eintragen.
    """

    KIND = None
    TITLE = ''

    def __init__(self, row: dict):
        self.id = row.get('source_id')
        self.name = row.get('name') or ''
        self.path = row.get('path') or ''
        self.exclude = parse_patterns(row.get('exclude'))
        self.interval = int(row.get('interval_s') or 0)
        self.row = row

    def validate(self):
        if self.name.strip() == '':
            raise ValueError('Name fehlt')
        if '/' in self.name or '\\' in self.name:
            raise ValueError('Name darf kein / oder \\ enthalten')
        if self.path.strip() == '':
            raise ValueError('Pfad fehlt')

    def files(self):
        raise NotImplementedError()

    def is_excluded(self, rel):
        name = rel.rsplit('/', 1)[-1]
        return any(fnmatch.fnmatch(name, p) or fnmatch.fnmatch(rel, p) for p in self.exclude)


class PathSource(Source):
    """ Verzeichnis mit Unterverzeichnissen, Ausschluss per Muster (z.B. *.tmp, .git, cache/*) """

    KIND = 'path'
    TITLE = 'Verzeichnis'

    def validate(self):
        super().validate()
        if not os.path.isdir(self.path):
            raise ValueError('Verzeichnis nicht gefunden: ' + self.path)

    def files(self):
        root = self.path
        for current, dirs, files in os.walk(root, onerror=lambda e: None):
            rel_dir = os.path.relpath(current, root).replace(os.sep, '/')
            rel_dir = '' if rel_dir == '.' else rel_dir + '/'
            dirs[:] = [d for d in dirs if not self.is_excluded(rel_dir + d)]
            for f in files:
                rel = rel_dir + f
                if self.is_excluded(rel):
                    continue
                full = os.path.join(current, f)
                try:
                    st = os.stat(full)
                except OSError:
                    continue
                yield rel, full, st


class FileSource(Source):
    """ Genau eine Datei """

    KIND = 'file'
    TITLE = 'Datei'

    def validate(self):
        super().validate()
        if not os.path.isfile(self.path):
            raise ValueError('Datei nicht gefunden: ' + self.path)

    def files(self):
        try:
            st = os.stat(self.path)
        except OSError:
            return
        yield os.path.basename(self.path), self.path, st


SOURCES = {c.KIND: c for c in [PathSource, FileSource]}


def parse_patterns(value):
    if not value:
        return []
    return [p.strip() for p in str(value).replace('\n', ',').split(',') if p.strip() != '']


def create_source(row: dict) -> Source:
    kind = row.get('kind')
    if kind not in SOURCES:
        raise ValueError('Unbekannte Quelle: ' + str(kind))
    return SOURCES[kind](row)
