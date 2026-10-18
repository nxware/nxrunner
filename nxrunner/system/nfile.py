import configparser
import html
import json
import os
import re
import threading
import time

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import web as w
from nwebclient import util as u


from nxrunner.system import Wsl


class NFile(r.BaseJobExecutor):
    """
      Indexiert die Dateien von Laufwerken in der Tabelle nfile der nweb-Persistence.

      - WSL: die Windows-Laufwerke unter /mnt/ (mounten ueber den Wsl-Runner mit op=update_drives)
      - Linux: die Dateisysteme aus /proc/mounts (ext4, btrfs, vfat, ntfs, nfs, cifs, ...), ohne /boot, /snap usw.

      Der Name eines Laufwerks kommt aus dem Eintrag name=... einer nweb.ini im Wurzelverzeichnis,
      ohne nweb.ini in der WSL aus dem Windows-Laufwerksnamen, unter Linux aus dem Dateisystem-Label
      bzw. dem Namen des Mountpoints. Dieser Name identifiziert das Laufwerk in der Datenbank.
      "Alle indexieren" beruecksichtigt nur Laufwerke mit nweb.ini. Beim Indexieren werden keine
      anderen Dateisysteme betreten (wie find -xdev).

      Benoetigt die nweb-Persistence (Arg DB), ohne funktioniert der Runner nicht.

      op=drives
      op=update_drives               mountet ueber den Wsl-Runner alle Windows-Laufwerke (nur WSL)
      op=index [drive=name]          indexiert ein oder alle Laufwerke (im Hintergrund)
               [path=sub/dir]        nur dieses Unterverzeichnis neu indexieren (nur mit drive)
               [exclude=a,b]         zusaetzlich auszuschliessende Verzeichnisnamen
      op=status                      Fortschritt der Indexierung
      op=wipe drive=name             loescht den Index eines Laufwerks
      op=index_list                  alle Laufwerke im Index mit Anzahl Dateien
      op=search q=text [drive=name]
      op=ls drive=name [path=sub/dir]
    """

    TAGS = ['system', 'files']

    TABLE = 'nfile'

    INI_NAME = 'nweb.ini'

    VOLUME_CACHE_S = 600

    # Verzeichnisse, die beim Indexieren nicht besucht werden (zusaetzlich zu allen, die mit . beginnen)
    EXCLUDE_DIRS = {'node_modules', 'site-packages', '__pycache__', '$RECYCLE.BIN', 'System Volume Information'}

    def __init__(self, type='nfile', search_limit=200):
        super().__init__(type)
        self.search_limit = int(search_limit)
        self.table_created = False
        self.index_thread = None
        self.index_status = {}
        self.volume_cache = None  # (zeitpunkt, volumes)
        self.volume_thread = None
        self.define_vars('search_limit')

    # --- nweb-Persistence ---

    def nweb(self):
        nw = getattr(self.getRoot(), 'nweb', None)
        if nw is None:
            raise RuntimeError('nweb-Persistence nicht verfuegbar (Arg DB setzen)')
        if not self.table_created:
            self.create_table(nw)
        return nw

    def create_table(self, nw):
        nw.dml(f"""CREATE TABLE IF NOT EXISTS {self.TABLE} (
            nfile_id INTEGER PRIMARY KEY AUTOINCREMENT,
            drive varchar(255) NOT NULL,
            path varchar(1024) NOT NULL,
            name varchar(255) NOT NULL,
            size INTEGER DEFAULT NULL,
            mtime INTEGER DEFAULT NULL,
            indexed INTEGER DEFAULT NULL
        )""")
        nw.dml(f"CREATE INDEX IF NOT EXISTS {self.TABLE}_name ON {self.TABLE} (name)")
        nw.dml(f"CREATE INDEX IF NOT EXISTS {self.TABLE}_drive ON {self.TABLE} (drive)")
        self.table_created = True

    @staticmethod
    def sql(nw, sql):
        """ ? auf den Platzhalter der Verbindung umstellen (fuer direkte Cursor-Zugriffe) """
        return sql.replace('?', nw.param) if nw.param != '?' else sql

    # --- Laufwerke ---

    def wsl(self) -> Wsl:
        d = self.getParentClass(r.LazyDispatcher)
        if d is not None:
            runners = d.byTag('wsl')
            if len(runners) > 0:
                return runners[0]
            runner = d.get_runner('wsl')
            if isinstance(runner, Wsl):
                return runner
        return Wsl()

    @staticmethod
    def read_drive_name(mountpoint):
        ini = os.path.join(mountpoint, NFile.INI_NAME)
        try:
            with open(ini, encoding='utf-8', errors='replace') as f:
                content = f.read()
        except OSError:
            return None
        cfg = configparser.ConfigParser(interpolation=None)
        try:
            cfg.read_string('[DEFAULT]\n' + content)
        except configparser.Error:
            return None
        for section in [cfg.default_section, *cfg.sections()]:
            name = cfg[section].get('name', '').strip()
            if name != '':
                return name
        return None

    def load_volumes(self, wsl):
        try:
            volumes = wsl.volumes()
        except Exception as e:
            self.error('Windows-Laufwerksnamen: ' + str(e))
            return None
        self.volume_cache = (time.time(), volumes)
        try:
            self.setting_set('volumes', json.dumps(volumes))
        except Exception as e:
            self.error('Windows-Laufwerksnamen speichern: ' + str(e))
        return volumes

    def load_volumes_async(self, wsl):
        if self.volume_thread is None or not self.volume_thread.is_alive():
            self.volume_thread = threading.Thread(target=self.load_volumes, args=(wsl,), daemon=True)
            self.volume_thread.start()

    def windows_volumes(self, wsl, wait=False):
        """
          Windows-Laufwerksnamen. Die Abfrage ueber powershell.exe ist langsam (warm ~4 s, kalt ~1 min),
          daher werden die Namen in den nweb-Settings gespeichert und nach VOLUME_CACHE_S im Hintergrund
          aktualisiert.
          Sind noch keine Namen bekannt, wird mit wait=True auf die Abfrage gewartet, sonst None geliefert
          (Abfrage laeuft im Hintergrund), damit z.B. Seiten nicht blockieren.
        """
        if self.volume_cache is None:
            try:
                saved = self.setting('volumes')
                if saved:
                    self.volume_cache = (0, json.loads(saved))
            except Exception as e:
                self.error('Windows-Laufwerksnamen laden: ' + str(e))
        if self.volume_cache is None:
            if not wait:
                self.load_volumes_async(wsl)
                return None
            if self.volume_thread is not None and self.volume_thread.is_alive():
                self.volume_thread.join()
            volumes = self.volume_cache[1] if self.volume_cache is not None else self.load_volumes(wsl)
            if volumes is None:
                # fehlgeschlagen: nicht bei jedem Aufruf erneut warten, sondern im Hintergrund wiederholen
                self.volume_cache = (0, {})
            return volumes or {}
        if time.time() - self.volume_cache[0] > self.VOLUME_CACHE_S:
            self.load_volumes_async(wsl)
        return self.volume_cache[1]

    @staticmethod
    def windows_name(letter, volume):
        if volume.get('label', '') != '':
            return volume['label']
        if volume.get('provider', '') != '':
            return volume['provider'].rstrip('\\').split('\\')[-1]
        return letter + ':'

    def is_wsl(self):
        return Wsl.is_wsl()

    def drives(self, wait=False):
        """
          [{source, mountpoint, fstype, name, name_source}] - name_source: nweb.ini, windows, label oder mount.
          In der WSL ist name None (name_source 'pending'), solange die Windows-Laufwerksnamen noch geladen
          werden, ausser bei wait=True. So entsteht kein Index unter einem vorlaeufigen Namen.
        """
        res = self.wsl_drives(wait) if self.is_wsl() else self.linux_drives()
        # gleiche Fallback-Namen (z.B. zwei mal "Volume") unterscheiden
        names = [d['name'] for d in res]
        for d in res:
            if d['name'] is not None and d['name_source'] != self.INI_NAME and names.count(d['name']) > 1:
                d['name'] = f"{d['name']} ({d['letter']}:)" if d.get('letter') else f"{d['name']} ({d['mountpoint']})"
        return res

    def wsl_drives(self, wait=False):
        """ Windows-Laufwerke unter /mnt/, ohne nweb.ini mit dem Windows-Laufwerksnamen """
        wsl = self.wsl()
        res = []
        for m in wsl.mounts():
            if not m['mountpoint'].startswith('/mnt/'):
                continue
            name = self.read_drive_name(m['mountpoint'])
            letter = m['source'][0].upper() if len(m['source']) >= 2 and m['source'][1] == ':' else None
            res.append(dict(**m, name=name, name_source=self.INI_NAME if name else None, letter=letter))
        volumes = self.windows_volumes(wsl, wait) if any(d['name'] is None for d in res) else {}
        for d in res:
            if d['name'] is not None:
                continue
            if volumes is None and d['letter']:
                d['name_source'] = 'pending'
            else:
                d['name'] = self.windows_name(d['letter'], volumes.get(d['letter'], {})) if d['letter'] else os.path.basename(d['mountpoint'])
                d['name_source'] = 'windows'
        return res

    # --- Linux ---

    # Dateisysteme, die unter Linux als Laufwerk gelten (tmpfs, proc, overlay usw. nicht)
    LINUX_FSTYPES = {'ext2', 'ext3', 'ext4', 'xfs', 'btrfs', 'zfs', 'f2fs', 'jfs', 'reiserfs', 'hfsplus',
                     'vfat', 'exfat', 'ntfs', 'ntfs3', 'fuseblk',
                     'nfs', 'nfs4', 'cifs', 'smb3', 'fuse.sshfs', 'fuse.rclone', '9p', 'virtiofs', 'drvfs'}

    # Mountpoints, die keine Laufwerke sind (inkl. Unterverzeichnissen), /run/media bleibt erlaubt
    LINUX_EXCLUDE_MOUNTS = ['/boot', '/snap', '/var/snap', '/var/lib/docker', '/var/lib/containers', '/proc', '/sys', '/dev']

    @staticmethod
    def read_mounts():
        res = []
        with open('/proc/mounts') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3:
                    res.append(dict(source=Wsl.unescape_mount(parts[0]), mountpoint=Wsl.unescape_mount(parts[1]), fstype=parts[2]))
        return res

    def is_linux_drive(self, m):
        mp = m['mountpoint']
        if m['fstype'] not in self.LINUX_FSTYPES:
            return False
        if mp.startswith('/run/') and not mp.startswith('/run/media/'):
            return False
        return not any(mp == p or mp.startswith(p + '/') for p in self.LINUX_EXCLUDE_MOUNTS)

    @staticmethod
    def linux_labels():
        """ Dateisystem-Labels: {Geraetepfad: Label} aus /dev/disk/by-label """
        res = {}
        base = '/dev/disk/by-label'
        try:
            for name in os.listdir(base):
                # udev kodiert Sonderzeichen, z.B. \x20 fuer Leerzeichen
                label = re.sub(r'\\x([0-9a-fA-F]{2})', lambda m: chr(int(m.group(1), 16)), name)
                res[os.path.realpath(os.path.join(base, name))] = label
        except OSError:
            pass
        return res

    @staticmethod
    def linux_name(m, labels):
        source = m['source']
        if source.startswith('/dev/') and os.path.realpath(source) in labels:
            return labels[os.path.realpath(source)], 'label'
        if source.startswith('//') or (':' in source and not source.startswith('/')):
            # Netzlaufwerk //server/share oder server:/export
            share = source.split(':', 1)[-1] if not source.startswith('//') else source
            last = share.replace('\\', '/').rstrip('/').split('/')[-1]
            if last != '':
                return last, 'mount'
        return (os.path.basename(m['mountpoint'].rstrip('/')) or 'root'), 'mount'

    def linux_drives(self):
        """ Dateisysteme aus /proc/mounts, ohne nweb.ini mit Label bzw. Mount-Namen """
        labels = None
        res = []
        for m in self.read_mounts():
            if not self.is_linux_drive(m):
                continue
            name = self.read_drive_name(m['mountpoint'])
            name_source = self.INI_NAME
            if name is None:
                if labels is None:
                    labels = self.linux_labels()
                name, name_source = self.linux_name(m, labels)
            res.append(dict(**m, name=name, name_source=name_source, letter=None))
        return res

    def drive_by_name(self, name):
        # wartet ggf. auf die Windows-Laufwerksnamen, damit ein Laufwerk sicher unter seinem Namen gefunden wird
        for d in self.drives(wait=True):
            if d['name'] == name:
                return d
        raise ValueError('Laufwerk nicht gefunden: ' + str(name))

    def execute_drives(self, data):
        return self.success('ok', value=self.drives())

    def execute_update_drives(self, data):
        if not self.is_wsl():
            return self.fail('update_drives ist nur in der WSL verfuegbar')
        wsl = self.wsl()
        res = wsl.execute_drives({})
        if not res['success']:
            return res
        mounted = []
        for drive in res['value']:
            m = wsl.execute_mount({'drive': drive})
            mounted.append(dict(drive=drive, success=m['success'], message=m['message'], stderr=m.get('stderr', '')))
        return self.success('ok', value=mounted, drives=self.drives(), ui_reload=True)

    # --- Indexieren ---

    def is_excluded(self, dirname, exclude=()):
        return dirname.startswith('.') or dirname in self.EXCLUDE_DIRS or dirname in exclude

    @staticmethod
    def resolve_path(mountpoint, path=''):
        """ Liefert den absoluten Pfad und den normalisierten relativen Pfad, nur innerhalb des Laufwerks """
        root = os.path.realpath(mountpoint)
        full = os.path.realpath(os.path.join(root, (path or '').strip().strip('/')))
        if full != root and not full.startswith(root + os.sep):
            raise ValueError('Pfad ausserhalb des Laufwerks')
        rel = os.path.relpath(full, root).replace(os.sep, '/')
        return full, '' if rel == '.' else rel

    @staticmethod
    def parse_exclude(value):
        if isinstance(value, (list, tuple, set)):
            return {v.strip() for v in value if v.strip() != ''}
        return {v.strip() for v in str(value or '').split(',') if v.strip() != ''}

    @staticmethod
    def same_fs(path, dev):
        try:
            return os.lstat(path).st_dev == dev
        except OSError:
            return False

    def walk(self, mountpoint, path='', exclude=()):
        start = os.path.join(mountpoint, path) if path else mountpoint
        # andere Dateisysteme (z.B. /proc oder weitere Laufwerke unter /) nicht betreten
        dev = os.stat(start).st_dev
        for root, dirs, files in os.walk(start, onerror=lambda e: None):
            dirs[:] = [d for d in dirs if not self.is_excluded(d, exclude) and self.same_fs(os.path.join(root, d), dev)]
            rel = os.path.relpath(root, mountpoint).replace(os.sep, '/')
            rel = '' if rel == '.' else rel
            for f in files:
                try:
                    st = os.stat(os.path.join(root, f))
                    yield rel, f, st.st_size, int(st.st_mtime)
                except OSError:
                    yield rel, f, None, None

    def scope_where(self, name, path=''):
        """ WHERE-Bedingung fuer ein Laufwerk oder ein Unterverzeichnis davon """
        if path == '':
            return "drive = ?", [name]
        # substr statt LIKE, da _ und % in Verzeichnisnamen vorkommen koennen
        prefix = path + '/'
        return "drive = ? AND (path = ? OR substr(path, 1, ?) = ?)", [name, path, len(prefix), prefix]

    def delete_index(self, nw, name, path=''):
        where, params = self.scope_where(name, path)
        nw.dml(f"DELETE FROM {self.TABLE} WHERE {where}", params)

    def count_index(self, nw, name, path=''):
        where, params = self.scope_where(name, path)
        return nw.select(f"SELECT COUNT(*) FROM {self.TABLE} WHERE {where}", params)[0][0]

    def index_drive(self, nw, drive, path='', exclude=(), batch_size=1000):
        name = drive['name']
        now = int(time.time())
        # Anzahl aus dem bisherigen Index als Schaetzung fuer den Fortschritt
        expected = self.count_index(nw, name, path)
        status = dict(state='running', count=0, expected=expected, start=now, path=path, dir='')
        self.index_status[name] = status
        insert = self.sql(nw, f"INSERT INTO {self.TABLE} (drive, path, name, size, mtime, indexed) VALUES (?, ?, ?, ?, ?, ?)")
        self.delete_index(nw, name, path)
        batch = []
        count = 0

        def flush():
            c = nw.connection.cursor()
            c.executemany(insert, batch)
            nw.connection.commit()
            c.close()
            batch.clear()

        for rel, f, size, mtime in self.walk(drive['mountpoint'], path, exclude):
            batch.append((name, rel, f, size, mtime, now))
            count += 1
            if len(batch) >= batch_size:
                flush()
                status['count'] = count
                status['dir'] = rel
        if batch:
            flush()
        self.index_status[name] = dict(state='done', count=count, expected=expected, start=now, path=path,
                                       duration=int(time.time()) - now)
        self.info(f"Index {name}/{path}: {count} Dateien")
        return count

    def index_drives(self, nw, drives, path='', exclude=()):
        for drive in drives:
            self.index_status[drive['name']] = dict(state='queued', path=path)
        for drive in drives:
            try:
                self.index_drive(nw, drive, path, exclude)
            except Exception as e:
                self.error(f"Index {drive['name']}: {e}")
                self.index_status[drive['name']] = dict(state='error', message=str(e))

    def execute_index(self, data):
        nw = self.nweb()
        if self.index_thread is not None and self.index_thread.is_alive():
            return self.fail('Indexierung laeuft bereits', status=self.index_status)
        exclude = self.parse_exclude(data.get('exclude'))
        path = ''
        if data.get('drive', '') != '':
            drives = [self.drive_by_name(data['drive'])]
            full, path = self.resolve_path(drives[0]['mountpoint'], data.get('path', ''))
            if not os.path.isdir(full):
                return self.fail('Verzeichnis nicht gefunden: ' + path)
        elif data.get('path', '') != '':
            return self.fail('path nur zusammen mit drive')
        else:
            # alle nur mit nweb.ini, damit nicht z.B. die komplette Systemplatte indexiert wird
            drives = [d for d in self.drives() if d['name_source'] == self.INI_NAME]
        if len(drives) == 0:
            return self.fail('Kein Laufwerk mit ' + self.INI_NAME + ' gefunden')
        self.index_thread = threading.Thread(target=self.index_drives, args=(nw, drives, path, exclude), daemon=True)
        self.index_thread.start()
        return self.success('Indexierung gestartet', drives=[d['name'] for d in drives], path=path, exclude=sorted(exclude))

    def is_indexing(self):
        return self.index_thread is not None and self.index_thread.is_alive()

    def execute_status(self, data):
        now = int(time.time())
        value = {k: {**v, 'elapsed': now - v['start']} if v.get('state') == 'running' else v for k, v in self.index_status.items()}
        return self.success('ok', running=self.is_indexing(), value=value)

    def execute_wipe(self, data):
        name = data.get('drive', '')
        if name == '':
            return self.fail('drive fehlt')
        nw = self.nweb()
        if self.is_indexing() and self.index_status.get(name, {}).get('state') in ('running', 'queued'):
            return self.fail('Laufwerk wird gerade indexiert: ' + name)
        count = self.count_index(nw, name)
        self.delete_index(nw, name)
        self.index_status.pop(name, None)
        return self.success(f'Index von {name} geloescht ({count} Dateien)', count=count, ui_reload=True)

    def index_overview(self):
        """ Alle Laufwerke im Index: [{drive, count, indexed}] """
        nw = self.nweb()
        rows = nw.select(f"SELECT drive, COUNT(*), MAX(indexed) FROM {self.TABLE} GROUP BY drive ORDER BY drive")
        return [dict(drive=row[0], count=row[1], indexed=row[2]) for row in rows]

    def execute_index_list(self, data):
        return self.success('ok', value=self.index_overview())

    # --- Abfragen ---

    def search(self, q, drive=None, limit=None):
        nw = self.nweb()
        sql = f"SELECT drive, path, name, size, mtime FROM {self.TABLE} WHERE name LIKE ?"
        params = ['%' + q + '%']
        if drive:
            sql += " AND drive = ?"
            params.append(drive)
        sql += f" ORDER BY drive, path, name LIMIT {int(limit or self.search_limit)}"
        rows = nw.select(sql, params)
        return [dict(drive=row[0], path=row[1], name=row[2], size=row[3], mtime=row[4]) for row in rows]

    def execute_search(self, data):
        if data.get('q', '') == '':
            return self.fail('q fehlt')
        return self.success('ok', value=self.search(data['q'], data.get('drive'), data.get('limit')))

    def execute_ls(self, data):
        drive = self.drive_by_name(data.get('drive'))
        path = self.resolve_path(drive['mountpoint'], data.get('path', ''))[0]
        res = []
        try:
            for e in os.scandir(path):
                res.append(dict(name=e.name, dir=e.is_dir(follow_symlinks=False)))
        except OSError as e:
            return self.fail(str(e))
        return self.success('ok', value=sorted(res, key=lambda x: (not x['dir'], x['name'].lower())))

    def execute_operation(self, data):
        try:
            return super().execute_operation(data)
        except (RuntimeError, ValueError) as e:
            return self.fail(str(e))

    # --- UI ---

    def index_btn(self, title, data):
        """ Startet die Indexierung und zeigt danach per JS-Handler den Fortschritt an """
        return self.action_btn_parametric(title, {'type': self.type, 'op': 'index', **data}, on_success='nfile_progress')

    def wipe_btn(self, name):
        msg = json.dumps(f'Index von "{name}" löschen?')
        job = json.dumps(dict(type=self.type, op='wipe', drive=name))
        return w.button_js("Wipe Index", f'if (confirm({msg})) {{ exec_job({job}); }}')

    def part_index(self, p: b.Page, params={}):
        p.h1("NFile")
        try:
            overview = self.index_overview()
        except Exception as e:
            p.div("Fehler: " + html.escape(str(e)))
            return
        counts = {o['drive']: o['count'] for o in overview}
        self.part_search(p, params)
        with p.section(h="Fortschritt"):
            p.div('', id='nfile_progress')
        drives = self.drives()
        wsl = self.is_wsl()
        name_sources = {'windows': 'Windows', 'label': 'Label', 'mount': 'Mount', 'pending': 'Windows-Name wird geladen …'}
        with p.section(h="Laufwerke"):
            p.div("Umgebung: " + ("WSL" if wsl else "Linux"))
            p('<table><tr><th>Mountpoint</th><th>Quelle</th><th>Name</th><th>Dateien</th><th></th></tr>')
            for d in drives:
                name = d['name']
                actions = ''
                if name:
                    actions = self.index_btn("Indexieren", dict(drive=name))
                    if name in counts:
                        actions += ' ' + self.wipe_btn(name)
                source = f" <small>({name_sources[d['name_source']]})</small>" if d['name_source'] in name_sources else ''
                p(f"<tr><td>{html.escape(d['mountpoint'])}</td><td>{html.escape(d['source'])}</td>"
                  f"<td>{html.escape(name or '')}{source}</td><td>{counts.get(name, '')}</td><td>{actions}</td></tr>")
            p('</table>')
            if wsl:
                p(self.action_btn(dict(title="Laufwerke aktualisieren (WSL mount)", type=self.type, op='update_drives')))
            p(self.index_btn("Alle mit " + self.INI_NAME + " indexieren", {}))
        self.part_index_list(p, overview, drives)
        self.part_index_form(p, drives)
        p.pre('', id='result')
        p.script(self.js_progress() + ('nfile_progress();' if self.is_indexing() else ''))

    def part_index_list(self, p: b.Page, overview, drives):
        mounted = {d['name'] for d in drives}
        with p.section(h="Index"):
            p.div(f"{sum(o['count'] for o in overview)} Dateien in {len(overview)} Laufwerken")
            p('<table><tr><th>Laufwerk</th><th>Dateien</th><th>Indexiert</th><th>Gemountet</th><th></th></tr>')
            for o in overview:
                p(f"<tr><td>{html.escape(o['drive'])}</td><td>{o['count']}</td><td>{u.format_datetime(o['indexed'])}</td>"
                  f"<td>{'ja' if o['drive'] in mounted else 'nein'}</td><td>{self.wipe_btn(o['drive'])}</td></tr>")
            p('</table>')

    def js_progress(self):
        """ JS-Handler: fragt per op=status den Fortschritt ab, bis die Indexierung fertig ist """
        return """
        var nfile_timer = null;
        function nfile_progress(result) {
          if (result && result.success === false) { show_error(result); return; }
          if (nfile_timer !== null) return;
          var poll = function() {
            exec_job({type: %s, op: 'status'}, function(s) {
              var $p = document.getElementById('nfile_progress');
              if ($p === null) { clearInterval(nfile_timer); nfile_timer = null; return; }
              $p.innerHTML = '';
              Object.keys(s.value || {}).forEach(function(name) {
                var v = s.value[name];
                var row = document.createElement('div');
                var text = ' ' + name + (v.path ? '/' + v.path : '') + ': ' + v.state;
                if (v.count !== undefined) text += ', ' + v.count + ' Dateien';
                if (v.state === 'running' && v.expected > 0) text += ' (ca. ' + Math.min(99, Math.round(100 * v.count / v.expected)) + '%% von ' + v.expected + ')';
                if (v.elapsed !== undefined) text += ', ' + v.elapsed + ' s';
                if (v.duration !== undefined) text += ', ' + v.duration + ' s';
                if (v.state === 'running' && v.dir) text += ', ' + v.dir;
                if (v.message) text += ', ' + v.message;
                row.textContent = text;
                if (v.state === 'running') {
                  var bar = document.createElement('progress');
                  if (v.expected > 0) { bar.max = v.expected; bar.value = Math.min(v.count, v.expected); }
                  row.prepend(bar);
                }
                $p.appendChild(row);
              });
              if (!s.running) {
                clearInterval(nfile_timer); nfile_timer = null;
                setTimeout(function() { location.reload(); }, 1500);
              }
            });
          };
          nfile_timer = setInterval(poll, 1000);
          poll();
        }
        """ % json.dumps(self.type)

    def part_index_form(self, p: b.Page, drives):
        names = [d['name'] for d in drives if d['name'] is not None]
        if len(names) == 0:
            return
        with p.section(h="Indexieren mit Optionen"):
            options = ''.join(f'<option value="{html.escape(n)}">{html.escape(n)}</option>' for n in names)
            p(f'<div><label for="nfile_drive">Laufwerk</label> <select id="nfile_drive">{options}</select></div>')
            p.form_input("nfile_path", "Unterverzeichnis", id='nfile_path', placeholder='leer = ganzes Laufwerk')
            p.form_input("nfile_exclude", "Zusätzlich ausschließen", id='nfile_exclude', placeholder='build, dist, tmp')
            p(self.index_btn("Indexieren", dict(drive='#nfile_drive', path='#nfile_path', exclude='#nfile_exclude')))
            p.div("Mit Unterverzeichnis wird nur dieses neu eingelesen, der übrige Index des Laufwerks bleibt erhalten. "
                  "Ausschlüsse sind Verzeichnisnamen (kommagetrennt), zusätzlich zu " + html.escape(', '.join(sorted(self.EXCLUDE_DIRS))) + " und .*")

    def part_search(self, p: b.Page, params={}):
        q = params.get('q', '')
        with p.section(h="Suche"):
            p(f'<form method="get">'
              f'<input type="hidden" name="type" value="{html.escape(self.type)}">'
              f'<input type="search" name="q" value="{html.escape(q)}" placeholder="Dateiname">'
              f'<input type="submit" value="Suchen">'
              f'</form>')
            if q != '':
                rows = self.search(q, params.get('drive'))
                p.div(f"{len(rows)} Treffer" + (f" (max. {self.search_limit})" if len(rows) >= self.search_limit else ''))
                p('<table><tr><th>Laufwerk</th><th>Pfad</th><th>Name</th><th>Größe</th></tr>')
                for row in rows:
                    p(f"<tr><td>{html.escape(row['drive'])}</td><td>{html.escape(row['path'])}</td>"
                      f"<td>{html.escape(row['name'])}</td><td>{u.format_size(row['size'])}</td></tr>")
                p('</table>')
