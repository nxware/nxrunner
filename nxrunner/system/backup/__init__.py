import html
import json
import threading
import time
import traceback
import weakref

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient.base import PeriodedThread
from nwebclient import web as w
from nwebclient import util as u

from nxrunner.system.backup.sources import SOURCES, create_source
from nxrunner.system.backup.destinations import DESTINATIONS, create_destination


class Backup(r.BaseJobExecutor):
    """
      Backup-System aus Quellen und Zielen, gespeichert in der nweb-Persistence (Arg DB).

      Quellen:  path (Verzeichnis mit Ausschlussmustern), file (einzelne Datei), ssh (Verzeichnis per SSH/SFTP,
                paramiko); je Quelle optional pre_script/post_script (bei ssh auf dem entfernten Rechner)
      Ziele:    drive (Laufwerksname wie in nfile + Pfad auf dem Medium), drive_zip (wie drive, aber in eine
                ZIP-Datei <Quelle>.zip, jeder Lauf schreibt sie neu, keep = Anzahl alter Versionen), smb (\\\\host\\share\\pfad)

      Eine Quelle kann mehrere Ziele haben (Tabelle backup_source_destination) und hat eine Haeufigkeit
      (interval_s, 0 = nur manuell). Faellig ist jedes Ziel einer Quelle fuer sich, letzter Lauf und Status
      stehen je Ziel in backup_source_destination, der zuletzt ausgefuehrte Lauf zusaetzlich an der Quelle.
      Die Dateien landen unter <Ziel>/<Name der Quelle>/<relativer Pfad>. Gesichert wird inkrementell:
      Groesse und Aenderungszeit jeder gesicherten Datei stehen je Ziel in backup_file, unveraenderte
      Dateien werden uebersprungen.

      op=sources | op=source_save kind name path [exclude] [interval_s] [destination_ids=1,2] [source_id]
      op=source_delete source_id
      op=destinations | op=destination_save kind name [drive path host share username password] [destination_id]
      op=destination_delete destination_id
      op=run source_id [destination_id]   Quelle jetzt sichern (alle bzw. ein Ziel)
      op=run_due                     alle faelligen Quellen/Ziele sichern (laeuft auch automatisch)
      op=status                      Fortschritt
      op=cancel                      laufendes Backup abbrechen (cancelled), ausstehende Ziele entfallen
      op=runs [source_id]            Historie der ausgefuehrten Backups (Tabelle backup_run)
      op=errors source_id            Fehlerdetails (Traceback) der letzten Laeufe, nur im Speicher

      Ziele sind nicht immer erreichbar (USB-Platte abgesteckt, NAS aus): Vor jedem Lauf wird das Ziel
      geprueft. Ist es offline, wird das am Ziel der Quelle vermerkt und es bleibt faellig, d.h. es
      wird gesichert, sobald das Ziel wieder erreichbar ist. Faellt das Ziel waehrend eines Laufs weg,
      wird abgebrochen (interrupted), der naechste Lauf setzt inkrementell fort.
      Ein gemeinsamer periodic-Thread prueft alle check_interval Sekunden, ob Quellen faellig sind.
    """

    TAGS = ['system', 'backup']

    INTERVALS = {0: 'manuell', 3600: 'stündlich', 86400: 'täglich', 604800: 'wöchentlich'}

    # in der UI bearbeitbare Felder einer Quelle (Source.FIELDS waehlt daraus je Art aus)
    SOURCE_EDIT_COLS = ['path', 'exclude', 'host', 'port', 'username', 'password', 'key_file', 'pre_script', 'post_script']
    SOURCE_COLS = ['source_id', 'kind', 'name', *SOURCE_EDIT_COLS, 'interval_s',
                   'last_run', 'last_status', 'last_message', 'size', 'files']
    # in der UI bearbeitbare Felder eines Ziels (Destination.FIELDS waehlt daraus je Art aus)
    DESTINATION_EDIT_COLS = ['drive', 'path', 'host', 'share', 'username', 'password', 'keep']
    # disk_*: zuletzt gemessene Groesse/Belegung des Laufwerks, wird bei Sicherungen und Anzeigen aktualisiert
    DESTINATION_COLS = ['destination_id', 'kind', 'name', *DESTINATION_EDIT_COLS, 'disk_total', 'disk_used', 'disk_checked']
    LINK_COLS = ['source_id', 'destination_id', 'last_run', 'last_status', 'last_message']

    # Ein gemeinsamer periodic-Thread fuer alle Backup-Instanzen, prueft regelmaessig, ob Quellen faellig sind
    scheduler = None
    scheduler_lock = threading.Lock()
    instances = weakref.WeakSet()

    def __init__(self, type='backup', check_interval=60):
        super().__init__(type)
        self.tables_created = False
        self.run_thread = None
        self.status = {}
        self.nfile = None
        # gesetzt von op=cancel, der laufende Lauf und alle noch ausstehenden werden abgebrochen
        self.cancel_event = threading.Event()
        # Status des letzten Laufs je (source_id, destination_id) inkl. Fehlerdetails, nur im Speicher (nicht in der DB)
        self.run_details = {}
        Backup.instances.add(self)
        Backup.start_scheduler(check_interval)

    @classmethod
    def start_scheduler(cls, check_interval=60):
        with cls.scheduler_lock:
            if cls.scheduler is None or not cls.scheduler.is_alive():
                # Daemon, damit der Thread das Beenden des Prozesses nicht blockiert
                cls.scheduler = PeriodedThread(cls.run_due_all, float(check_interval), start=False)
                cls.scheduler.daemon = True
                cls.scheduler.start()

    @classmethod
    def run_due_all(cls):
        for backup in list(cls.instances):
            try:
                backup.run_due()
            except Exception as e:
                backup.error('Scheduler: ' + str(e))

    # --- nweb-Persistence ---

    def nweb(self):
        nw = getattr(self.getRoot(), 'nweb', None)
        if nw is None:
            raise RuntimeError('nweb-Persistence nicht verfuegbar (Arg DB setzen)')
        if not self.tables_created:
            self.create_tables(nw)
        return nw

    def create_tables(self, nw):
        nw.dml("""CREATE TABLE IF NOT EXISTS backup_source (
            source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind varchar(32) NOT NULL,
            name varchar(255) NOT NULL,
            path varchar(1024) DEFAULT NULL,
            exclude text DEFAULT NULL,
            interval_s INTEGER DEFAULT 0,
            destination_id INTEGER DEFAULT NULL,
            last_run INTEGER DEFAULT NULL,
            last_status varchar(32) DEFAULT NULL,
            last_message text DEFAULT NULL
        )""")
        nw.dml("""CREATE TABLE IF NOT EXISTS backup_destination (
            destination_id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind varchar(32) NOT NULL,
            name varchar(255) NOT NULL,
            drive varchar(255) DEFAULT NULL,
            path varchar(1024) DEFAULT NULL,
            host varchar(255) DEFAULT NULL,
            share varchar(255) DEFAULT NULL,
            username varchar(255) DEFAULT NULL,
            password varchar(255) DEFAULT NULL,
            keep INTEGER DEFAULT NULL
        )""")
        self.add_column(nw, 'backup_destination', 'keep INTEGER DEFAULT NULL')
        for column in ['disk_total INTEGER DEFAULT NULL', 'disk_used INTEGER DEFAULT NULL', 'disk_checked INTEGER DEFAULT NULL']:
            self.add_column(nw, 'backup_destination', column)
        # Groesse der Quelle (Bytes, Anzahl Dateien), ermittelt beim letzten vollstaendigen Durchlauf einer Sicherung
        for column in ['size INTEGER DEFAULT NULL', 'files INTEGER DEFAULT NULL']:
            self.add_column(nw, 'backup_source', column)
        # entfernte Quellen (SSH) und Pre-/Post-Script
        for column in ['host varchar(255) DEFAULT NULL', 'port INTEGER DEFAULT NULL', 'username varchar(255) DEFAULT NULL',
                       'password varchar(255) DEFAULT NULL', 'key_file varchar(1024) DEFAULT NULL',
                       'pre_script text DEFAULT NULL', 'post_script text DEFAULT NULL']:
            self.add_column(nw, 'backup_source', column)
        # Ziele einer Quelle, mit letztem Lauf je Ziel
        nw.dml("""CREATE TABLE IF NOT EXISTS backup_source_destination (
            source_id INTEGER NOT NULL,
            destination_id INTEGER NOT NULL,
            last_run INTEGER DEFAULT NULL,
            last_status varchar(32) DEFAULT NULL,
            last_message text DEFAULT NULL
        )""")
        nw.dml("CREATE UNIQUE INDEX IF NOT EXISTS backup_source_destination_sd ON backup_source_destination (source_id, destination_id)")
        # Migration: frueher hatte eine Quelle genau ein Ziel (backup_source.destination_id, wird nicht mehr verwendet)
        nw.dml("""INSERT INTO backup_source_destination (source_id, destination_id, last_run, last_status, last_message)
            SELECT source_id, destination_id, last_run, last_status, last_message FROM backup_source s
            WHERE destination_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM backup_source_destination l WHERE l.source_id = s.source_id AND l.destination_id = s.destination_id)""")
        nw.dml("UPDATE backup_source SET destination_id = NULL WHERE destination_id IS NOT NULL")
        nw.dml("""CREATE TABLE IF NOT EXISTS backup_file (
            backup_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id INTEGER NOT NULL,
            destination_id INTEGER NOT NULL,
            path varchar(1024) NOT NULL,
            size INTEGER DEFAULT NULL,
            mtime INTEGER DEFAULT NULL,
            backed_up INTEGER DEFAULT NULL
        )""")
        nw.dml("CREATE INDEX IF NOT EXISTS backup_file_sd ON backup_file (source_id, destination_id, path)")
        # Historie der ausgefuehrten Backups
        nw.dml("""CREATE TABLE IF NOT EXISTS backup_run (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id INTEGER NOT NULL,
            destination_id INTEGER DEFAULT NULL,
            started INTEGER NOT NULL,
            finished INTEGER DEFAULT NULL,
            status varchar(32) DEFAULT NULL,
            count INTEGER DEFAULT 0,
            copied INTEGER DEFAULT 0,
            skipped INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            bytes INTEGER DEFAULT 0,
            message text DEFAULT NULL
        )""")
        nw.dml("CREATE INDEX IF NOT EXISTS backup_run_source ON backup_run (source_id, started)")
        self.tables_created = True

    @staticmethod
    def add_column(nw, table, column_ddl):
        """ Spalte in bestehenden Tabellen nachruesten, ist sie schon vorhanden, passiert nichts """
        try:
            nw.dml(f"ALTER TABLE {table} ADD COLUMN {column_ddl}")
        except Exception as e:
            if 'duplicate' not in str(e).lower():
                raise

    @staticmethod
    def sql(nw, sql):
        """ ? auf den Platzhalter der Verbindung umstellen (fuer direkte Cursor-Zugriffe) """
        return sql.replace('?', nw.param) if nw.param != '?' else sql

    def select_dicts(self, nw, sql, params, cols):
        return [dict(zip(cols, row)) for row in nw.select(sql, params)]

    def get_sources(self, nw):
        return self.select_dicts(nw, f"SELECT {', '.join(self.SOURCE_COLS)} FROM backup_source ORDER BY name", [], self.SOURCE_COLS)

    def get_source(self, nw, source_id):
        rows = self.select_dicts(nw, f"SELECT {', '.join(self.SOURCE_COLS)} FROM backup_source WHERE source_id = ?",
                                 [int(source_id)], self.SOURCE_COLS)
        if len(rows) == 0:
            raise ValueError('Quelle nicht gefunden: ' + str(source_id))
        return rows[0]

    def get_destinations(self, nw):
        return self.select_dicts(nw, f"SELECT {', '.join(self.DESTINATION_COLS)} FROM backup_destination ORDER BY name", [],
                                 self.DESTINATION_COLS)

    def get_destination(self, nw, destination_id):
        rows = self.select_dicts(nw, f"SELECT {', '.join(self.DESTINATION_COLS)} FROM backup_destination WHERE destination_id = ?",
                                 [int(destination_id)], self.DESTINATION_COLS)
        if len(rows) == 0:
            raise ValueError('Ziel nicht gefunden: ' + str(destination_id))
        return rows[0]

    def get_links(self, nw, source_id=None, destination_id=None):
        """ Ziele von Quellen: [{source_id, destination_id, last_run, last_status, last_message}] """
        where, params = [], []
        if source_id is not None:
            where.append('source_id = ?')
            params.append(int(source_id))
        if destination_id is not None:
            where.append('destination_id = ?')
            params.append(int(destination_id))
        sql = f"SELECT {', '.join(self.LINK_COLS)} FROM backup_source_destination"
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        return self.select_dicts(nw, sql + ' ORDER BY source_id, destination_id', params, self.LINK_COLS)

    def set_links(self, nw, source_id, destination_ids):
        """ Ziele einer Quelle setzen, bestehende behalten ihren letzten Lauf """
        existing = {link['destination_id'] for link in self.get_links(nw, source_id)}
        for destination_id in existing - set(destination_ids):
            # Metadaten verwerfen, wird das Ziel spaeter wieder zugeordnet, wird vollstaendig gesichert
            nw.dml("DELETE FROM backup_file WHERE source_id = ? AND destination_id = ?", [source_id, destination_id])
            nw.dml("DELETE FROM backup_source_destination WHERE source_id = ? AND destination_id = ?", [source_id, destination_id])
        for destination_id in [d for d in destination_ids if d not in existing]:
            nw.dml("INSERT INTO backup_source_destination (source_id, destination_id) VALUES (?, ?)", [source_id, destination_id])

    @staticmethod
    def public_destination(row):
        """ Ziel ohne Passwort, fuer Ausgaben """
        return {**row, 'password': '***' if row.get('password') else ''}

    # --- Laufwerke (ueber nfile) ---

    def get_nfile(self):
        from nxrunner.system.nfile import NFile
        d = self.getParentClass(r.LazyDispatcher)
        if d is not None:
            runners = [runner for runner in d.loaded() if isinstance(runner, NFile)]
            if len(runners) > 0:
                return runners[0]
        if self.nfile is None:
            # als Child, damit die eigene NFile-Instanz ueber getRoot() die nweb-Settings erreicht
            # (dort werden die langsam abzufragenden Windows-Laufwerksnamen gespeichert)
            self.nfile = NFile()
            self.addChild(self.nfile)
        return self.nfile

    def drives(self):
        try:
            return self.get_nfile().drives()
        except Exception as e:
            self.error('Laufwerke: ' + str(e))
            return []

    def drive_mountpoint(self, name):
        for d in self.drives():
            if d['name'] == name:
                return d['mountpoint']
        return None

    # --- Quellen und Ziele verwalten ---

    def execute_sources(self, data):
        nw = self.nweb()
        links = self.get_links(nw)
        return self.success('ok', value=[{**self.public_destination(s), 'destinations': [link for link in links if link['source_id'] == s['source_id']]}
                                         for s in self.get_sources(nw)])

    @staticmethod
    def parse_ids(value):
        """ '1,3' bzw. [1, 3] -> [1, 3] """
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            return [int(v) for v in value if str(v).strip() != '']
        return [int(v) for v in str(value).split(',') if v.strip() != '']

    def execute_source_save(self, data):
        nw = self.nweb()
        kind = data.get('kind', '')
        if kind not in SOURCES:
            raise ValueError('Unbekannte Quelle: ' + kind)
        fields = SOURCES[kind].FIELDS
        row = dict(kind=kind, name=data.get('name', '').strip(), interval_s=int(data.get('interval_s') or 0))
        for c in self.SOURCE_EDIT_COLS:
            # nicht verwendete Felder leeren, Passwort und Scripte unveraendert (Leerzeichen/Zeilen sind gewollt)
            value = data.get(c, '') if c in fields else None
            row[c] = value.strip() if isinstance(value, str) and c not in ('password', 'pre_script', 'post_script') else value
        if row['port'] is not None:
            if str(row['port']).strip() not in ('',) and not str(row['port']).strip().isdigit():
                raise ValueError('Port: Zahl erwartet')
            row['port'] = int(row['port']) if str(row['port']).strip() != '' else None
        source_id = data.get('source_id', '')
        if source_id != '' and row.get('password') == '':
            # leeres Passwort beim Bearbeiten: bisheriges behalten
            row['password'] = self.get_source(nw, source_id)['password']
        create_source(row).validate()
        destination_ids = self.parse_ids(data.get('destination_ids'))
        for destination_id in destination_ids:
            self.get_destination(nw, destination_id)
        same = [s for s in self.get_sources(nw) if s['name'] == row['name'] and str(s['source_id']) != str(source_id)]
        if same:
            raise ValueError('Name bereits vergeben: ' + row['name'])
        cols = list(row.keys())
        if source_id != '':
            source_id = int(source_id)
            self.get_source(nw, source_id)
            nw.dml(f"UPDATE backup_source SET {', '.join(c + ' = ?' for c in cols)} WHERE source_id = ?",
                   [row[c] for c in cols] + [source_id])
        else:
            nw.dml(f"INSERT INTO backup_source ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})", [row[c] for c in cols])
            source_id = [s for s in self.get_sources(nw) if s['name'] == row['name']][0]['source_id']
        self.set_links(nw, source_id, destination_ids)
        return self.success('Quelle gespeichert', source_id=source_id, ui_reload=True)

    def execute_source_delete(self, data):
        nw = self.nweb()
        source_id = int(data['source_id'])
        if self.is_running() and self.status.get('source_id') == source_id:
            raise ValueError('Quelle wird gerade gesichert')
        nw.dml("DELETE FROM backup_file WHERE source_id = ?", [source_id])
        nw.dml("DELETE FROM backup_source_destination WHERE source_id = ?", [source_id])
        nw.dml("DELETE FROM backup_source WHERE source_id = ?", [source_id])
        return self.success('Quelle geloescht (die gesicherten Dateien in den Zielen bleiben erhalten)', ui_reload=True)

    def execute_destinations(self, data):
        return self.success('ok', value=[self.public_destination(d) for d in self.get_destinations(self.nweb())])

    def execute_destination_save(self, data):
        nw = self.nweb()
        kind = data.get('kind', '')
        if kind not in DESTINATIONS:
            raise ValueError('Unbekanntes Ziel: ' + kind)
        fields = DESTINATIONS[kind].FIELDS
        row = {'kind': kind, 'name': data.get('name', '').strip()}
        for c in self.DESTINATION_EDIT_COLS:
            row[c] = data.get(c, '').strip() if c in fields and c != 'password' else (data.get(c, '') if c in fields else None)
        if row['keep'] is not None:
            if not str(row['keep']).isdigit() and row['keep'] != '':
                raise ValueError('Alte Versionen: ganze Zahl >= 0 erwartet')
            row['keep'] = int(row['keep'] or 0)
        destination_id = data.get('destination_id', '')
        if destination_id != '' and row.get('password') == '':
            # leeres Passwort beim Bearbeiten: bisheriges behalten
            row['password'] = self.get_destination(nw, destination_id)['password']
        create_destination(row, self).validate()
        cols = list(row.keys())
        if destination_id != '':
            nw.dml(f"UPDATE backup_destination SET {', '.join(c + ' = ?' for c in cols)} WHERE destination_id = ?",
                   [row[c] for c in cols] + [int(destination_id)])
        else:
            nw.dml(f"INSERT INTO backup_destination ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})", [row[c] for c in cols])
        return self.success('Ziel gespeichert', ui_reload=True)

    def execute_destination_delete(self, data):
        nw = self.nweb()
        destination_id = int(data['destination_id'])
        names = {s['source_id']: s['name'] for s in self.get_sources(nw)}
        used = [names.get(link['source_id'], str(link['source_id'])) for link in self.get_links(nw, destination_id=destination_id)]
        if used:
            raise ValueError('Ziel wird noch verwendet von: ' + ', '.join(used))
        nw.dml("DELETE FROM backup_file WHERE destination_id = ?", [destination_id])
        nw.dml("DELETE FROM backup_destination WHERE destination_id = ?", [destination_id])
        return self.success('Ziel geloescht', ui_reload=True)

    # --- Sichern ---

    def is_running(self):
        return self.run_thread is not None and self.run_thread.is_alive()

    def is_due(self, source, link, now):
        """ Ziel einer Quelle faellig: nicht nur manuell und Intervall seit dem letzten Lauf zu diesem Ziel abgelaufen """
        interval = int(source['interval_s'] or 0)
        return interval > 0 and (link['last_run'] is None or now - int(link['last_run']) >= interval)

    def start(self, nw, jobs):
        """ jobs: [(Quelle, Ziel)] nacheinander sichern """
        if self.is_running():
            raise ValueError('Es laeuft bereits ein Backup')
        self.cancel_event.clear()
        self.run_thread = threading.Thread(target=self.run_jobs, args=(nw, jobs), daemon=True)
        self.run_thread.start()

    def execute_cancel(self, data):
        """ laufendes Backup abbrechen, noch ausstehende Ziele werden nicht mehr gesichert """
        if not self.is_running():
            return self.fail('Es laeuft kein Backup')
        self.cancel_event.set()
        return self.success('Abbruch angefordert')

    def check_destination(self, nw, destination_id):
        """ :return: (erreichbar, Meldung, Ziel-Zeile) """
        try:
            dest_row = self.get_destination(nw, destination_id)
            available, message = create_destination(dest_row, self).check()
            return available, message, dest_row
        except Exception as e:
            return False, str(e), None

    def mark_offline(self, nw, link, message):
        """ Ziel nicht erreichbar: vermerken, last_run bleibt, damit das Ziel faellig bleibt und spaeter gesichert wird """
        if link.get('last_status') != 'offline' or link.get('last_message') != message:
            nw.dml("UPDATE backup_source_destination SET last_status = ?, last_message = ? WHERE source_id = ? AND destination_id = ?",
                   ['offline', message, link['source_id'], link['destination_id']])

    def collect_jobs(self, nw, sources, links, filter_fn):
        """ (Quelle, Ziel) fuer alle Ziele, auf die filter_fn(source, link) zutrifft und die erreichbar sind """
        checked = {}
        jobs, offline = [], []
        for source in sources:
            source_checked = None
            for link in [link for link in links if link['source_id'] == source['source_id']]:
                if not filter_fn(source, link):
                    continue
                # entfernte Quelle (z.B. SSH) nicht erreichbar: wie ein offline-Ziel vermerken, bleibt faellig
                if source_checked is None:
                    try:
                        source_checked = create_source(source).check()
                    except Exception as e:
                        source_checked = (False, str(e))
                if not source_checked[0]:
                    self.mark_offline(nw, link, source_checked[1])
                    offline.append(source_checked[1])
                    continue
                if link['destination_id'] not in checked:
                    checked[link['destination_id']] = self.check_destination(nw, link['destination_id'])
                available, message, dest_row = checked[link['destination_id']]
                if available:
                    jobs.append((source, dest_row))
                else:
                    self.mark_offline(nw, link, message)
                    offline.append(message)
        return jobs, offline

    def run_due(self):
        """ Vom Scheduler aufgerufen, sichert alle faelligen Ziele von Quellen, die erreichbar sind """
        if getattr(self.getRoot(), 'nweb', None) is None or self.is_running():
            return []
        nw = self.nweb()
        now = int(time.time())
        jobs, offline = self.collect_jobs(nw, self.get_sources(nw), self.get_links(nw), lambda s, link: self.is_due(s, link, now))
        if jobs:
            self.start(nw, jobs)
        return [f"{source['name']} -> {dest['name']}" for source, dest in jobs]

    def execute_run(self, data):
        nw = self.nweb()
        source = self.get_source(nw, data['source_id'])
        destination_id = data.get('destination_id', '')
        links = self.get_links(nw, source['source_id'], destination_id if destination_id != '' else None)
        if len(links) == 0:
            return self.fail('Quelle hat kein Ziel: ' + source['name'])
        jobs, offline = self.collect_jobs(nw, [source], links, lambda s, link: True)
        # gleiche Meldung je Ziel (z.B. Quelle offline) nur einmal
        offline = list(dict.fromkeys(offline))
        if not jobs:
            return self.fail('; '.join(offline))
        self.start(nw, jobs)
        message = 'Backup gestartet: ' + source['name'] + ' -> ' + ', '.join(dest['name'] for _, dest in jobs)
        if offline:
            message += ' (nicht erreichbar: ' + '; '.join(offline) + ')'
        return self.success(message)

    def execute_run_due(self, data):
        self.nweb()
        names = self.run_due()
        return self.success('Gestartet: ' + ', '.join(names) if names else 'Nichts faellig bzw. Ziel nicht erreichbar', value=names)

    def execute_status(self, data):
        status = dict(self.status)
        if status.get('state') == 'running':
            status['elapsed'] = int(time.time()) - status['start']
        return self.success('ok', running=self.is_running(), value=status)

    RUN_COLS = ['run_id', 'source_id', 'destination_id', 'started', 'finished', 'status', 'count', 'copied', 'skipped',
                'failed', 'bytes', 'message']

    def get_runs(self, nw, source_id=None, limit=50):
        where, params = ('WHERE source_id = ?', [int(source_id)]) if source_id not in (None, '') else ('', [])
        return self.select_dicts(nw, f"SELECT {', '.join(self.RUN_COLS)} FROM backup_run {where} ORDER BY started DESC, run_id DESC LIMIT {int(limit)}",
                                 params, self.RUN_COLS)

    def execute_runs(self, data):
        """ Historie der Backups, optional fuer eine Quelle """
        return self.success('ok', value=self.get_runs(self.nweb(), data.get('source_id'), data.get('limit') or 50))

    def run_jobs(self, nw, jobs):
        """
          jobs: [(Quelle, Ziel)]. Je Quelle wird einmal verbunden (open), das Pre-Script ausgefuehrt, in alle
          ihre Ziele gesichert, danach das Post-Script ausgefuehrt (auch nach Fehlern) und geschlossen.
        """
        groups = []
        for row, dest_row in jobs:
            if groups and groups[-1][0]['source_id'] == row['source_id']:
                groups[-1][1].append(dest_row)
            else:
                groups.append((row, [dest_row]))
        for row, dest_rows in groups:
            if self.cancel_event.is_set():
                self.info(f"Backup {row['name']}: wegen Abbruch nicht ausgefuehrt")
                continue
            try:
                self.run_source_group(nw, row, dest_rows)
            except Exception as e:
                self.error(f"Backup {row['name']}: {e}")

    @staticmethod
    def script_error(name, code, output):
        return {'file': '', 'error': f'{name} fehlgeschlagen (Exit-Code {code})', 'trace': output[-10000:]}

    def run_source_group(self, nw, row, dest_rows):
        source = create_source(row)
        try:
            source.open()
        except Exception as e:
            error = {'file': '', 'error': f'{type(e).__name__}: {e}', 'trace': traceback.format_exc()}
            for dest_row in dest_rows:
                self.record_failure(nw, row, dest_row, f'Quelle nicht verfuegbar: {e}', error)
            source.close()
            return
        try:
            if source.pre_script:
                code, output = source.run_script(source.pre_script)
                if code != 0:
                    error = self.script_error('Pre-Script', code, output)
                    for dest_row in dest_rows:
                        self.record_failure(nw, row, dest_row, f'Pre-Script fehlgeschlagen (Exit-Code {code}), nicht gesichert', error)
                    return
            for dest_row in dest_rows:
                if self.cancel_event.is_set():
                    self.info(f"Backup {row['name']} -> {dest_row['name']}: wegen Abbruch nicht ausgefuehrt")
                    continue
                self.run_source(nw, row, dest_row, source=source)
        finally:
            try:
                if source.post_script:
                    code, output = source.run_script(source.post_script)
                    if code != 0:
                        self.post_script_failed(nw, row, dest_rows, self.script_error('Post-Script', code, output), code)
            except Exception as e:
                self.post_script_failed(nw, row, dest_rows, {'file': '', 'error': f'Post-Script: {type(e).__name__}: {e}',
                                                             'trace': traceback.format_exc()}, None)
            source.close()

    def post_script_failed(self, nw, row, dest_rows, error, code):
        """ Post-Script fehlgeschlagen: an den Laeufen (Fehlerdetails) und an der Quelle vermerken """
        for dest_row in dest_rows:
            status = self.run_details.get((row['source_id'], dest_row['destination_id']))
            if status is not None:
                status['errors'] = (status['errors'] + [error])[-self.MAX_ERRORS:]
        nw.dml("UPDATE backup_source SET last_status = ?, last_message = ? WHERE source_id = ?",
               ['errors', f"Post-Script fehlgeschlagen (Exit-Code {code})", row['source_id']])
        self.error(f"Backup {row['name']}: {error['error']}")

    def store_usage(self, nw, destination):
        """ Groesse/Belegung des Mediums speichern, wenn ermittelbar; sonst bleibt der letzte bekannte Stand """
        try:
            usage = destination.usage()
        except Exception as e:
            self.error('Belegung: ' + str(e))
            usage = None
        if usage is not None:
            nw.dml("UPDATE backup_destination SET disk_total = ?, disk_used = ?, disk_checked = ? WHERE destination_id = ?",
                   [usage[0], usage[1], int(time.time()), destination.id])
        return usage

    # so viele Fehlerdetails werden je Lauf im Speicher gehalten
    MAX_ERRORS = 100

    def add_error(self, status, rel, e):
        """
          Fehlerdetails fuer die Anzeige (nur im Speicher, nicht in der DB). Muss im except-Block aufgerufen
          werden, damit der Traceback verfuegbar ist.
        """
        error = {'file': rel, 'error': f'{type(e).__name__}: {e}', 'trace': traceback.format_exc()}
        for attr in ('errno', 'filename', 'filename2'):
            if getattr(e, attr, None) is not None:
                error[attr] = str(getattr(e, attr))
        status['errors'] = (status['errors'] + [error])[-self.MAX_ERRORS:]

    def execute_errors(self, data):
        """ Fehlerdetails der letzten Laeufe einer Quelle (aus dem Speicher) """
        source_id = int(data['source_id'])
        return self.success('ok', value=[s for (sid, _), s in self.run_details.items() if sid == source_id])

    def new_status(self, row, dest_row, now):
        sid, did = row['source_id'], dest_row['destination_id']
        status = dict(state='running', source_id=sid, source=row['name'], destination_id=did, destination=dest_row['name'],
                      start=now, count=0, size=0, copied=0, skipped=0, failed=0, bytes=0, current='', errors=[])
        self.status = status
        self.run_details[(sid, did)] = status
        return status

    def record_failure(self, nw, row, dest_row, message, error):
        """ Lauf fuer ein Ziel gar nicht moeglich (z.B. Verbindung zur Quelle, Pre-Script): als error vermerken """
        now = int(time.time())
        status = self.new_status(row, dest_row, now)
        status['errors'] = [error]
        self.finish_run(nw, row, dest_row, status, 'error', message, now)

    def finish_run(self, nw, row, dest_row, status, result, message, now, destination=None):
        """ Lauf in backup_run, am Ziel der Quelle und an der Quelle vermerken """
        sid, did = row['source_id'], dest_row['destination_id']
        finished = int(time.time())
        if result == 'interrupted':
            nw.dml("UPDATE backup_source_destination SET last_status = ?, last_message = ? WHERE source_id = ? AND destination_id = ?",
                   [result, message, sid, did])
        else:
            nw.dml("UPDATE backup_source_destination SET last_run = ?, last_status = ?, last_message = ? WHERE source_id = ? AND destination_id = ?",
                   [now, result, message, sid, did])
        # zuletzt ausgefuehrter Lauf an der Quelle
        nw.dml("UPDATE backup_source SET last_run = ?, last_status = ?, last_message = ? WHERE source_id = ?",
               [now, result, f"{dest_row['name']}: {message}", sid])
        if result in ('ok', 'errors'):
            # Quelle vollstaendig durchlaufen: Groesse aktualisieren
            nw.dml("UPDATE backup_source SET size = ?, files = ? WHERE source_id = ?", [status['size'], status['count'], sid])
        if destination is not None:
            self.store_usage(nw, destination)
        nw.dml("INSERT INTO backup_run (source_id, destination_id, started, finished, status, count, copied, skipped, failed, bytes, message) "
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
               [sid, did, now, finished, result, status['count'], status['copied'],
                status['skipped'], status['failed'], status['bytes'], message])
        status.update(state={'error': 'error', 'interrupted': 'interrupted', 'cancelled': 'cancelled'}.get(result, 'done'),
                      result=result, message=message, duration=finished - now, current='')
        self.info(f"Backup {row['name']} -> {dest_row['name']}: {result} {message}")
        return status

    def run_source(self, nw, row, dest_row, batch_size=200, source=None):
        """
          Sichert eine Quelle in ein Ziel. Ergebnis (status):
            ok, errors (einzelne Dateien fehlgeschlagen), error (Lauf fehlgeschlagen),
            interrupted (Ziel waehrend des Laufs weggefallen), offline (Ziel beim Start nicht erreichbar)
          Ausser bei offline wird der Lauf in backup_run, am Ziel der Quelle und an der Quelle vermerkt. Bei
          interrupted und offline bleibt last_run des Ziels unveraendert, damit es faellig bleibt und fortgesetzt
          wird, sobald das Ziel erreichbar ist.
        """
        now = int(time.time())
        sid, did = row['source_id'], dest_row['destination_id']
        link = {'source_id': sid, 'destination_id': did}
        status = self.new_status(row, dest_row, now)
        result, message = 'error', ''
        destination = None
        # ohne uebergebene Quelle (direkter Aufruf) selbst oeffnen und schliessen, sonst macht das run_jobs
        own_source = source is None
        try:
            if own_source:
                source = create_source(row)
                source.open()
            destination = create_destination(dest_row, self)
            available, check_message = destination.check()
            if not available:
                self.mark_offline(nw, link, check_message)
                status.update(state='offline', result='offline', message=check_message)
                return status
            # nicht inkrementelle Ziele (z.B. ZIP) bekommen in jedem Lauf alle Dateien
            incremental = destination.INCREMENTAL
            known = {r[0]: (r[1], r[2]) for r in nw.select(
                "SELECT path, size, mtime FROM backup_file WHERE source_id = ? AND destination_id = ?", [sid, did])} if incremental else {}
            destination.open(source)
            pending = []

            def flush():
                c = nw.connection.cursor()
                c.executemany(self.sql(nw, "DELETE FROM backup_file WHERE source_id = ? AND destination_id = ? AND path = ?"),
                              [(sid, did, p[0]) for p in pending])
                c.executemany(self.sql(nw, "INSERT INTO backup_file (source_id, destination_id, path, size, mtime, backed_up) VALUES (?, ?, ?, ?, ?, ?)"),
                              [(sid, did, *p) for p in pending])
                nw.connection.commit()
                c.close()
                pending.clear()

            interrupted = None
            cancelled = False
            try:
                for rel, full, st in source.files():
                    if self.cancel_event.is_set():
                        cancelled = True
                        break
                    status['count'] += 1
                    status['size'] += st.st_size
                    meta = (st.st_size, int(st.st_mtime))
                    # unveraendert und im Ziel noch vorhanden (z.B. nicht dort geloescht) -> ueberspringen
                    if known.get(rel) == meta and destination.has(rel, st.st_size):
                        status['skipped'] += 1
                        continue
                    status['current'] = rel
                    local = None
                    try:
                        # entfernte Quellen laden die Datei hier in eine temporaere lokale Datei
                        local = source.local_path(rel, full, st)
                        destination.copy(rel, local, st)
                        pending.append((rel, *meta, int(time.time())))
                        status['copied'] += 1
                        status['bytes'] += st.st_size
                    except Exception as e:
                        # Ziel weggefallen (abgesteckt, Netzwerk weg)? Dann abbrechen statt jede Datei scheitern zu lassen
                        available, check_message = destination.check()
                        if not available:
                            interrupted = check_message
                            break
                        status['failed'] += 1
                        self.add_error(status, rel, e)
                    finally:
                        if local is not None:
                            source.release(rel, local)
                    if incremental and len(pending) >= batch_size:
                        flush()
            finally:
                # bereits kopierte Dateien vermerken, damit ein Abbruch beim naechsten Lauf fortgesetzt wird
                if incremental and pending:
                    flush()
            if not interrupted and not cancelled:
                destination.commit()
                if not incremental:
                    # vollstaendige Sicherung abgeschlossen: Metadaten durch den Stand dieses Laufs ersetzen
                    nw.dml("DELETE FROM backup_file WHERE source_id = ? AND destination_id = ?", [sid, did])
                    flush()
            counts = f"{status['copied']} kopiert, {status['skipped']} unveraendert, {status['failed']} Fehler"
            if interrupted:
                result, message = 'interrupted', f'Abgebrochen: {interrupted} ({counts})'
            elif cancelled:
                # ohne commit: eine angefangene ZIP wird verworfen, inkrementell kopierte Dateien bleiben vermerkt
                result, message = 'cancelled', f'Vom Benutzer abgebrochen ({counts})'
            else:
                result = 'ok' if status['failed'] == 0 else 'errors'
                message = counts
        except Exception as e:
            message = str(e)
            self.add_error(status, '', e)
        finally:
            if destination is not None:
                try:
                    destination.close()
                except Exception:
                    pass
            if own_source and source is not None:
                source.close()
        self.finish_run(nw, row, dest_row, status, result, message, now, destination)
        return status

    def execute_operation(self, data):
        try:
            return super().execute_operation(data)
        except (RuntimeError, ValueError, KeyError) as e:
            return self.fail(str(e))

    # --- UI ---

    def interval_title(self, seconds):
        seconds = int(seconds or 0)
        return self.INTERVALS.get(seconds, f'alle {seconds} s')

    def link_index(self):
        return f'?type={self.type}'

    def link_source(self, source_id):
        """ Detailseite einer Quelle (part_source) """
        return f'?type={self.type}&{self.type}=source&source_id={int(source_id)}'

    def part_source(self, p: b.Page, params={}):
        """ Detailseite einer Quelle: Ziele mit Stand, Sichern, Bearbeiten und die letzten Sicherungen """
        p.h1("Backup")
        p(w.button_js("Zurück zur Übersicht", f'location.href = {json.dumps(self.link_index())};'))
        if params.get('source_id', '') == '':
            p.div("Keine Quelle angegeben.")
            return
        try:
            nw = self.nweb()
            source = self.get_source(nw, params['source_id'])
            destinations = self.get_destinations(nw)
            dest_names = {d['destination_id']: d['name'] for d in destinations}
            stats = self.backup_stats(nw)
            sid = source['source_id']
            with p.section(h="Quelle: " + source['name']):
                p.div("Größe: " + self.html_source_size(source))
                p.div(self.html_source_targets(source, self.get_links(nw, sid), dest_names, stats))
                p(self.action_btn_parametric("Jetzt sichern", dict(type=self.type, op='run', source_id=sid), on_success='backup_progress'))
                p.div('', id='backup_progress')
            self.part_run_errors(p, sid)
            self.part_create_source(p, params)
            self.part_runs(p, nw, {sid: source['name']}, dest_names, source_id=sid, title="Letzte Sicherungen")
        except Exception as e:
            p.div("Fehler: " + html.escape(str(e)))
        p.pre('', id='result')
        p.script(self.js_progress() + ('backup_progress();' if self.is_running() else ''))

    def link_due(self):
        """ Seite der faelligen Sicherungen (part_due) """
        return f'?type={self.type}&{self.type}=due'

    def part_due(self, p: b.Page, params={}):
        """ faellige Sicherungen (Quelle -> Ziel), am laengsten ueberfaellige zuerst, ohne automatische Aktualisierung """
        p.h1("Backup: fällige Sicherungen")
        p(w.button_js("Zurück zur Übersicht", f'location.href = {json.dumps(self.link_index())};'))
        try:
            nw = self.nweb()
            sources = {s['source_id']: s for s in self.get_sources(nw)}
            dest_names = {d['destination_id']: d['name'] for d in self.get_destinations(nw)}
            links = self.get_links(nw)
        except Exception as e:
            p.div("Fehler: " + html.escape(str(e)))
            return
        now = int(time.time())
        # je Quelle ein Eintrag mit ihren faelligen Zielen
        due = {}
        for link in links:
            source = sources.get(link['source_id'])
            if source is not None and self.is_due(source, link, now):
                due.setdefault(source['source_id'], (source, []))[1].append(link)

        def due_at(source, link):
            """ Zeitpunkt der Faelligkeit, nie gesichert: None """
            return None if link['last_run'] is None else int(link['last_run']) + int(source['interval_s'])

        def first_due(entry):
            source, due_links = entry
            times = [due_at(source, link) for link in due_links]
            return -1 if None in times else min(times)

        # nie gesichert zuerst, dann am laengsten ueberfaellig
        entries = sorted(due.values(), key=first_due)
        if not entries:
            p.div("Keine Sicherung fällig.")
        else:
            p('<table><tr><th>Quelle</th><th>Fällig</th><th>Häufigkeit</th><th>Fällige Ziele</th></tr>')
            for source, due_links in entries:
                first = first_due((source, due_links))
                since = "noch nie gesichert" if first == -1 else                     f"<span title=\"{u.format_datetime(first)}\">{u.format_age(first, now)}</span>"
                p(f"<tr><td>{w.a(html.escape(source['name']), self.link_source(source['source_id']))}</td>"
                  f"<td>{since}</td><td>{html.escape(self.interval_title(source['interval_s']))}</td>"
                  f"<td>{self.html_link_states(due_links, dest_names)}</td></tr>")
            p('</table>')
            p.div(f"{len(entries)} Quellen fällig. Gesichert wird automatisch, sobald Quelle und Ziel erreichbar sind.")
            p(self.action_btn_parametric("Fällige jetzt sichern", dict(type=self.type, op='run_due')))
        self.part_manual(p, sources, links, dest_names)
        p.pre('', id='result')

    def part_manual(self, p: b.Page, sources: dict, links, dest_names):
        """ Quellen, die nur manuell gesichert werden, mit ihrer letzten Sicherung (zuletzt gesicherte zuerst) """
        manual = [s for s in sources.values() if int(s['interval_s'] or 0) <= 0]
        with p.section(h="Manuell"):
            if not manual:
                p.div("Keine Quellen, die nur manuell gesichert werden.")
                return
            entries = []
            for source in manual:
                source_links = [link for link in links if link['source_id'] == source['source_id']]
                runs = [int(link['last_run']) for link in source_links if link['last_run'] is not None]
                entries.append((max(runs) if runs else None, source, source_links))
            entries.sort(key=lambda e: -(e[0] or 0))
            p('<table><tr><th>Quelle</th><th>Letzte Sicherung</th><th>Ziele</th></tr>')
            for last, source, source_links in entries:
                last_text = "noch nie gesichert" if last is None else                     f"<span title=\"{u.format_datetime(last)}\">{u.format_age(last)}</span>"
                p(f"<tr><td>{w.a(html.escape(source['name']), self.link_source(source['source_id']))}</td>"
                  f"<td>{last_text}</td><td>{self.html_link_states(source_links, dest_names) if source_links else '–'}</td></tr>")
            p('</table>')

    @staticmethod
    def html_link_states(links, dest_names):
        """ je Ziel eine Zeile: Name, letzte Sicherung und ggf. letzter Status """
        items = []
        for link in links:
            text = f"<b>{html.escape(dest_names.get(link['destination_id'], str(link['destination_id'])))}</b>: "
            if link['last_run'] is None:
                text += "noch nie gesichert"
            else:
                text += f"letzte Sicherung <span title=\"{u.format_datetime(link['last_run'])}\">{u.format_age(link['last_run'])}</span>"
            if link['last_status']:
                text += f", {html.escape(link['last_status'])}: {html.escape(link['last_message'] or '')}"
            items.append(f'<div>{text}</div>')
        return ''.join(items)

    def part_run_errors(self, p: b.Page, source_id):
        """ Fehlerdetails der letzten Laeufe einer Quelle seit dem Start des Servers (nur im Speicher) """
        runs = [s for (sid, _), s in self.run_details.items() if sid == source_id and s.get('errors')]
        if not runs:
            return
        with p.section(h="Fehler des letzten Laufs"):
            for status in runs:
                p.div(f"<b>{html.escape(status['destination'])}</b>, {u.format_age(status['start'])}: "
                      f"{html.escape(status.get('message') or status.get('state', ''))}")
                p(''.join(self.html_error(e) for e in status['errors']))
            p.div("Die Details werden nicht gespeichert und gehen bei einem Neustart verloren.")

    @staticmethod
    def html_error(error):
        """ ein Fehler aufklappbar: Datei und Meldung, darunter Traceback """
        summary = (error.get('file') + ': ' if error.get('file') else '') + error.get('error', '')
        extra = ''.join(f"{k}: {error[k]}\n" for k in ('errno', 'filename', 'filename2') if k in error)
        return f"<details><summary>{html.escape(summary)}</summary><pre>{html.escape(extra + error.get('trace', ''))}</pre></details>"

    def backup_stats(self, nw):
        """ {(source_id, destination_id): (Anzahl Dateien, Groesse)} der gesicherten Dateien """
        return {(row[0], row[1]): (row[2], row[3]) for row in nw.select(
            "SELECT source_id, destination_id, COUNT(*), SUM(size) FROM backup_file GROUP BY source_id, destination_id")}

    def part_index(self, p: b.Page, params={}):
        p.h1("Backup")
        try:
            nw = self.nweb()
            sources = self.get_sources(nw)
            destinations = self.get_destinations(nw)
            links = self.get_links(nw)
            stats = self.backup_stats(nw)
        except Exception as e:
            p.div("Fehler: " + html.escape(str(e)))
            return
        dest_names = {d['destination_id']: d['name'] for d in destinations}
        p.div(w.a("Fällige Sicherungen", self.link_due()))
        with p.section(h="Fortschritt"):
            p.div('', id='backup_progress')
        with p.section(h="Quellen"):
            p('<table><tr><th>Name</th><th>Art</th><th>Pfad</th><th>Ausschluss</th><th>Häufigkeit</th>'
              '<th>Größe</th><th>Ziele</th><th></th></tr>')
            for s in sources:
                p(f"<tr><td>{w.a(html.escape(s['name']), self.link_source(s['source_id']))}</td>"
                  f"<td>{html.escape(SOURCES[s['kind']].TITLE if s['kind'] in SOURCES else s['kind'])}</td>"
                  f"<td>{html.escape(self.source_description(s))}</td><td>{html.escape(s['exclude'] or '')}</td>"
                  f"<td>{html.escape(self.interval_title(s['interval_s']))}</td>"
                  f"<td>{self.html_source_size(s)}</td>"
                  f"<td>{self.html_source_targets(s, [link for link in links if link['source_id'] == s['source_id']], dest_names, stats)}</td>"
                  f"<td>{self.html_source_actions(s)}</td></tr>")
            p('</table>')
        self.part_create_source(p, {})
        with p.section(h="Ziele"):
            p('<table><tr><th>Name</th><th>Art</th><th>Ziel</th><th>Status</th><th>Belegt</th><th></th></tr>')
            for d in destinations:
                dest = create_destination(d, self)
                available, message = dest.check(timeout=1)
                state = 'erreichbar' if available else 'nicht erreichbar'
                if available:
                    # erreichbar: Belegung neu messen und speichern, sonst den zuletzt gespeicherten Stand zeigen
                    usage = self.store_usage(nw, dest)
                    if usage is not None:
                        d = {**d, 'disk_total': usage[0], 'disk_used': usage[1], 'disk_checked': int(time.time())}
                delete = self.confirm_btn("Löschen", f"Ziel {d['name']} löschen?", dict(type=self.type, op='destination_delete', destination_id=d['destination_id']))
                p(f"<tr><td>{html.escape(d['name'])}</td><td>{html.escape(dest.TITLE)}</td>"
                  f"<td>{html.escape(dest.describe())}</td><td title=\"{html.escape(message)}\">{state}</td>"
                  f"<td>{self.html_disk_usage(d)}</td><td>{delete}</td></tr>")
            p('</table>')
        self.part_create_target(p, params)
        self.part_runs(p, nw, {s['source_id']: s['name'] for s in sources}, dest_names)
        p.pre('', id='result')
        p.script(self.js_progress() + ('backup_progress();' if self.is_running() else ''))

    def html_source_targets(self, source, links, dest_names, stats):
        """ Ziele einer Quelle mit Stand der Sicherung und letztem Lauf """
        if len(links) == 0:
            return '–'
        items = []
        for link in links:
            count, size = stats.get((source['source_id'], link['destination_id']), (0, 0))
            text = f"<b>{html.escape(dest_names.get(link['destination_id'], str(link['destination_id'])))}</b>: {count} Dateien, {u.format_size(size or 0)}"
            if link['last_run']:
                text += f", letzte Sicherung <span title=\"{u.format_datetime(link['last_run'])}\">{u.format_age(link['last_run'])}</span>"
            else:
                text += ", noch nie gesichert"
            if link['last_status']:
                text += f" {html.escape(link['last_status'])}: {html.escape(link['last_message'] or '')}"
            if self.run_details.get((source['source_id'], link['destination_id']), {}).get('errors'):
                text += ' ' + w.a("Fehlerdetails", self.link_source(source['source_id']))
            items.append(f'<div>{text}</div>')
        return ''.join(items)

    @staticmethod
    def source_description(row):
        try:
            return create_source(row).describe()
        except Exception:
            return row.get('path') or ''

    @staticmethod
    def html_source_size(source):
        """ gespeicherte Groesse der Quelle, '–' solange noch keine Sicherung vollstaendig durchgelaufen ist """
        if source.get('size') is None:
            return '–'
        return f"{u.format_size(source['size'])} ({source.get('files') or 0} Dateien)"

    @staticmethod
    def html_disk_usage(dest_row):
        """ gespeicherte Belegung eines Laufwerksziels, z.B. '120 GB von 500 GB (24 %)', mit Stand im Tooltip """
        total, used = dest_row.get('disk_total'), dest_row.get('disk_used')
        if not total:
            return '–'
        percent = round(100 * (used or 0) / total)
        checked = dest_row.get('disk_checked')
        title = f"Stand {u.format_datetime(checked)}, {u.format_size(total - (used or 0))} frei" if checked else ''
        text = f"{u.format_size(used or 0)} von {u.format_size(total)} ({percent} %)"
        if checked and time.time() - checked > 3600:
            text += f" <small>({u.format_age(checked)})</small>"
        return f'<span title="{html.escape(title, quote=True)}">{text}</span>'

    def html_source_actions(self, source):
        sid = source['source_id']
        return self.action_btn_parametric("Sichern", dict(type=self.type, op='run', source_id=sid), on_success='backup_progress') \
            + ' ' + w.button_js("Details", f'location.href = {json.dumps(self.link_source(sid))};') \
            + ' ' + self.confirm_btn("Löschen", f"Quelle {source['name']} löschen?", dict(type=self.type, op='source_delete', source_id=sid))

    def part_runs(self, p: b.Page, nw, source_names, dest_names, limit=20, source_id=None, title="Letzte Läufe"):
        runs = self.get_runs(nw, source_id, limit=limit)
        with p.section(h=title):
            if len(runs) == 0:
                p.div("Noch keine Sicherungen.")
                return
            p('<table><tr><th>Start</th><th>Dauer</th><th>Quelle</th><th>Ziel</th><th>Status</th><th>Dateien</th>'
              '<th>Kopiert</th><th>Meldung</th></tr>')
            for run in runs:
                p(f"<tr><td title=\"{u.format_datetime(run['started'])}\">{u.format_age(run['started'])}</td>"
                  f"<td>{(run['finished'] or run['started']) - run['started']} s</td>"
                  f"<td>{html.escape(source_names.get(run['source_id'], str(run['source_id'])))}</td>"
                  f"<td>{html.escape(dest_names.get(run['destination_id'], str(run['destination_id'])))}</td>"
                  f"<td>{html.escape(run['status'] or '')}</td><td>{run['count']}</td>"
                  f"<td>{run['copied']} ({u.format_size(run['bytes'] or 0)})</td><td>{html.escape(run['message'] or '')}</td></tr>")
            p('</table>')

    def confirm_btn(self, title, question, job):
        return w.button_js(title, f'if (confirm({json.dumps(question)})) {{ exec_job({json.dumps(job)}); }}')

    def destination_tooltip(self, row):
        """ Tooltip eines Ziels, z.B. 'Laufwerk: usb-platte:/backup/pc1' """
        try:
            dest = create_destination(row, self)
            return f"{dest.TITLE}: {dest.describe()}"
        except Exception as e:
            return str(e)

    def part_create_source(self, p: b.Page, params={}):
        """
          Formular: Quelle anlegen bzw. mit source_id bearbeiten. Ziele per Checkbox, eine Quelle kann
          mehrere oder (vorerst) keine Ziele haben.
        """
        nw = self.nweb()
        destinations = self.get_destinations(nw)
        source, selected = {}, []
        if params.get('source_id', '') != '':
            source = self.get_source(nw, params['source_id'])
            selected = [link['destination_id'] for link in self.get_links(nw, source['source_id'])]
        value = lambda key: html.escape(str(source.get(key) or ''), quote=True)
        title = f"Quelle bearbeiten: {source['name']}" if source else "Quelle anlegen"
        with p.section(h=title):
            p('<a id="bs_form"></a>')
            p.hidden('source_id', source.get('source_id', ''), id='bs_source_id')
            p.form_select('kind', "Art", {k: c.TITLE for k, c in SOURCES.items()}, source.get('kind'), id='bs_kind',
                          onchange='backup_source_fields()')
            p.form_input('name', "Name", id='bs_name', placeholder='Ordner im Ziel, z.B. dokumente', value=value('name'))
            p.form_input('host', "Host", id='bs_host', placeholder='server.local', value=value('host'))
            p.form_input('port', "Port", id='bs_port', type='number', placeholder='22', value=value('port'))
            p.form_input('username', "Benutzer", id='bs_username', value=value('username'))
            # Passwort nie ausgeben; leer lassen = bisheriges behalten
            p.form_input('password', "Passwort", id='bs_password', type='password', autocomplete='new-password',
                         placeholder='unverändert' if source.get('password') else 'leer = Schlüssel verwenden')
            p.form_input('key_file', "Schlüsseldatei", id='bs_key_file', placeholder='leer = ~/.ssh bzw. ssh-agent',
                         value=value('key_file'))
            p.form_input('path', "Pfad", id='bs_path', placeholder='/home/user/Dokumente oder Datei', value=value('path'))
            p.form_input('exclude', "Ausschluss", id='bs_exclude', placeholder='*.tmp, .git, cache/*', value=value('exclude'))
            for key, label, hint in [('pre_script', 'Pre-Script', 'vor der Sicherung, z.B. Datenbank-Dump; Fehler = nicht sichern'),
                                     ('post_script', 'Post-Script', 'nach der Sicherung, läuft auch nach Fehlern')]:
                p.form_elem(w.textarea(html.escape(source.get(key) or ''), name=key, id='bs_' + key, rows='3', cols='50',
                                       placeholder=hint), label)
            p.form_select('interval_s', "Häufigkeit", self.INTERVALS, source.get('interval_s', 86400), id='bs_interval')
            checks = ''.join(
                f'<label title="{html.escape(self.destination_tooltip(d), quote=True)}">'
                f'<input type="checkbox" class="bs_dest" value="{d["destination_id"]}"'
                f'{" checked" if d["destination_id"] in selected else ""} onchange="backup_source_dests()"> '
                f'{html.escape(d["name"])}</label> ' for d in destinations)
            p.form_elem((checks or '(noch keine Ziele angelegt)') + '<input type="hidden" id="bs_destination_ids">', "Ziele")
            p(self.action_btn_parametric("Quelle speichern", dict(
                type=self.type, op='source_save', source_id='#bs_source_id', kind='#bs_kind', name='#bs_name', path='#bs_path',
                exclude='#bs_exclude', interval_s='#bs_interval', destination_ids='#bs_destination_ids',
                host='#bs_host', port='#bs_port', username='#bs_username', password='#bs_password', key_file='#bs_key_file',
                pre_script='#bs_pre_script', post_script='#bs_post_script')))
            p.div("Ausschluss: kommagetrennte Muster, geprüft gegen Datei-/Verzeichnisname und relativen Pfad. "
                  "Ohne Ziel wird die Quelle nicht gesichert. "
                  "Wird ein Ziel abgewählt, wird beim erneuten Zuordnen vollständig gesichert. "
                  "Bei SSH laufen Pre-/Post-Script auf dem entfernten Rechner, sonst lokal; "
                  "der Host-Schlüssel muss in ~/.ssh/known_hosts stehen. Das Passwort wird im Klartext in der DB gespeichert.")
            p.script(self.js_fields('backup_source_fields', 'bs_', {k: c.FIELDS for k, c in SOURCES.items()},
                                    self.SOURCE_EDIT_COLS) + """
            function backup_source_dests() {
              var ids = [];
              document.querySelectorAll('.bs_dest').forEach(function(c) { if (c.checked) ids.push(c.value); });
              document.getElementById('bs_destination_ids').value = ids.join(',');
            }
            backup_source_dests();
            backup_source_fields();
            """)

    @staticmethod
    def js_fields(function_name, prefix, fields, all_fields):
        """ JS: blendet Formularfelder (Zeilen von form_input/form_elem) passend zur gewaehlten Art ein und aus """
        return """
        function %s() {
          var fields = %s;
          var kind = document.getElementById('%skind').value;
          var visible = fields[kind] || [];
          %s.forEach(function(f) {
            var el = document.getElementById('%s' + f);
            var row = el ? el.closest('.page_form_input') : null;
            if (row) row.style.display = visible.indexOf(f) >= 0 ? '' : 'none';
          });
        }
        """ % (function_name, json.dumps(fields), prefix, json.dumps(all_fields), prefix)

    # Felder des Ziel-Formulars, die je nach Art (Destination.FIELDS) ein- und ausgeblendet werden
    TARGET_FIELDS = ['drive', 'path', 'keep', 'host', 'share', 'username', 'password']

    def part_create_target(self, p: b.Page, params={}):
        """ Formular: Ziel anlegen, Felder werden passend zur Art ein- und ausgeblendet """
        drives = [d['name'] for d in self.drives() if d.get('name')]
        with p.section(h="Ziel anlegen"):
            p.form_select('kind', "Art", {k: c.TITLE for k, c in DESTINATIONS.items()}, id='bd_kind', onchange='backup_target_fields()')
            p.form_input('name', "Name", id='bd_name', placeholder='z.B. USB-Platte')
            p.form_input('drive', "Laufwerk", id='bd_drive', list='bd_drives')
            p('<datalist id="bd_drives">' + ''.join(f'<option value="{html.escape(n)}">' for n in drives) + '</datalist>')
            p.form_input('path', "Pfad", id='bd_path', placeholder='backup/pc1')
            # form_input verwendet title als Beschriftung, fuer den Tooltip daher ein eigenes input
            keep_hint = 'Anzahl vorheriger ZIP-Dateien, die behalten werden (<Quelle>.1.zip ...), 0 = nur die aktuelle'
            p.form_elem(f'<input name="keep" id="bd_keep" type="number" min="0" value="0" title="{html.escape(keep_hint, quote=True)}">',
                        "Alte Versionen behalten")
            p.form_input('host', "Host", id='bd_host', placeholder='nas.local')
            p.form_input('share', "Freigabe", id='bd_share', placeholder='backup')
            p.form_input('username', "Benutzer", id='bd_username')
            p.form_input('password', "Passwort", id='bd_password', type='password', autocomplete='new-password')
            p(self.action_btn_parametric("Ziel speichern", dict(
                type=self.type, op='destination_save', kind='#bd_kind', name='#bd_name', drive='#bd_drive', path='#bd_path',
                keep='#bd_keep', host='#bd_host', share='#bd_share', username='#bd_username', password='#bd_password')))
            p.div("Das SMB-Passwort wird im Klartext in der nweb-Datenbank gespeichert.", id='bd_password_hint')
            p.script(self.js_target_fields() + 'backup_target_fields();')

    def js_target_fields(self):
        """ JS: blendet die Felder des Ziel-Formulars passend zur gewaehlten Art ein und aus """
        fields = {k: c.FIELDS for k, c in DESTINATIONS.items()}
        return """
        function backup_target_fields() {
          var fields = %s;
          var kind = document.getElementById('bd_kind').value;
          var visible = fields[kind] || [];
          %s.forEach(function(f) {
            var el = document.getElementById('bd_' + f);
            var row = el ? el.closest('.page_form_input') : null;
            if (row) row.style.display = visible.indexOf(f) >= 0 ? '' : 'none';
          });
          var hint = document.getElementById('bd_password_hint');
          if (hint) hint.style.display = visible.indexOf('password') >= 0 ? '' : 'none';
        }
        """ % (json.dumps(fields), json.dumps(self.TARGET_FIELDS))

    def js_progress(self):
        """ JS-Handler: fragt per op=status den Fortschritt ab, bis das Backup fertig ist """
        return """
        var backup_timer = null;
        var backup_type = %s;
        var backup_cancel_requested = false;
        function backup_cancel(btn) {
          backup_cancel_requested = true;
          btn.disabled = true;
          btn.textContent = 'Wird abgebrochen …';
          exec_job({type: backup_type, op: 'cancel'}, function(r) {});
        }
        function backup_progress(result) {
          if (result && result.success === false) { show_error(result); return; }
          if (backup_timer !== null) return;
          var poll = function() {
            exec_job({type: backup_type, op: 'status'}, function(s) {
              var $p = document.getElementById('backup_progress');
              if ($p === null) { clearInterval(backup_timer); backup_timer = null; return; }
              var v = s.value || {};
              // aufgeklappte Fehler bleiben beim Aktualisieren offen
              var opened = [].map.call($p.querySelectorAll('details[open] summary'), function(e) { return e.textContent; });
              $p.innerHTML = '';
              if (v.state) {
                var row = document.createElement('div');
                var text = ' ' + v.source + ' -> ' + v.destination + ': ' + v.state + ', ' + v.count + ' Dateien, '
                  + v.copied + ' kopiert, ' + v.skipped + ' unverändert, ' + v.failed + ' Fehler';
                if (v.elapsed !== undefined) text += ', ' + v.elapsed + ' s';
                if (v.duration !== undefined) text += ', ' + v.duration + ' s';
                if (v.current) text += ', ' + v.current;
                if (v.message) text += ' - ' + v.message;
                row.textContent = text;
                if (v.state === 'running') row.prepend(document.createElement('progress'));
                $p.appendChild(row);
                (v.errors || []).forEach(function(e) {
                  // aufklappbar: Datei und Meldung, darunter errno/Dateinamen und Traceback
                  var err = document.createElement('details');
                  var summary = document.createElement('summary');
                  summary.textContent = (e.file ? e.file + ': ' : '') + e.error;
                  var pre = document.createElement('pre');
                  pre.textContent = ['errno', 'filename', 'filename2'].filter(function(k) { return e[k] !== undefined; })
                    .map(function(k) { return k + ': ' + e[k] + '\\n'; }).join('') + (e.trace || '');
                  err.open = opened.indexOf(summary.textContent) >= 0;
                  err.appendChild(summary);
                  err.appendChild(pre);
                  $p.appendChild(err);
                });
              }
              if (s.running) {
                var btn = document.createElement('button');
                btn.textContent = backup_cancel_requested ? 'Wird abgebrochen …' : 'Abbrechen';
                btn.disabled = backup_cancel_requested;
                btn.onclick = function() { backup_cancel(btn); };
                $p.appendChild(btn);
              }
              if (!s.running) {
                clearInterval(backup_timer); backup_timer = null;
                setTimeout(function() { location.reload(); }, 2000);
              }
            });
          };
          backup_timer = setInterval(poll, 1000);
          poll();
        }
        """ % json.dumps(self.type)
