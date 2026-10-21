import fnmatch
import os
import posixpath
import shutil
import socket
import stat as stat_module
import subprocess
import tempfile


class Source:
    """
      Quelle eines Backups. Liefert die zu sichernden Dateien als (relativer Pfad, Pfad, stat).
      Neue Arten: Unterklasse mit KIND anlegen und in SOURCES eintragen.

      Ablauf je Lauf: check(), open(), pre_script, je Ziel files() mit local_path()/release() je kopierter Datei,
      post_script, close(). Die Ziele kopieren immer von einer lokalen Datei (local_path).
    """

    KIND = None
    TITLE = ''
    # Spalten aus backup_source, die diese Art verwendet (Formular blendet die uebrigen aus)
    FIELDS = ['path', 'exclude', 'pre_script', 'post_script']
    SCRIPT_TIMEOUT = 3600

    def __init__(self, row: dict):
        self.id = row.get('source_id')
        self.name = row.get('name') or ''
        self.path = row.get('path') or ''
        self.exclude = parse_patterns(row.get('exclude'))
        self.interval = int(row.get('interval_s') or 0)
        self.pre_script = (row.get('pre_script') or '').strip()
        self.post_script = (row.get('post_script') or '').strip()
        self.row = row

    def validate(self):
        if self.name.strip() == '':
            raise ValueError('Name fehlt')
        if '/' in self.name or '\\' in self.name:
            raise ValueError('Name darf kein / oder \\ enthalten')
        if self.path.strip() == '':
            raise ValueError('Pfad fehlt')

    def describe(self):
        return self.path

    def check(self, timeout=3):
        """ :return: (erreichbar, Meldung), z.B. entfernte Quellen """
        return True, 'ok'

    def open(self):
        pass

    def close(self):
        pass

    def files(self):
        raise NotImplementedError()

    def local_path(self, rel, path, st):
        """ lokale Datei zum Kopieren, entfernte Quellen laden sie hier in eine temporaere Datei """
        return path

    def release(self, rel, local):
        """ nach dem Kopieren, entfernt z.B. die temporaere Datei """
        pass

    def run_script(self, script):
        """ Pre-/Post-Script lokal ausfuehren. :return: (Exit-Code, Ausgabe) """
        p = subprocess.run(['bash', '-c', script], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           timeout=self.SCRIPT_TIMEOUT)
        return p.returncode, p.stdout.decode('utf-8', errors='replace')

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
    FIELDS = ['path', 'pre_script', 'post_script']

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


class SshDirSource(Source):
    """
      Verzeichnis auf einem entfernten Rechner per SSH/SFTP (paramiko).
      Anmeldung: key_file, sonst password, sonst Schluessel aus ~/.ssh bzw. ssh-agent.
      Host-Aliase aus ~/.ssh/config werden aufgeloest (HostName, Port, User, IdentityFile), Angaben in der
      Quelle (port, username, key_file) haben Vorrang.
      Der Host-Schluessel muss bekannt sein (~/.ssh/known_hosts, z.B. einmal per ssh verbinden).
      Pre-/Post-Script laufen auf dem entfernten Rechner (z.B. Datenbank-Dump vorher, Aufraeumen danach).
    """

    KIND = 'ssh'
    TITLE = 'SSH-Verzeichnis'
    FIELDS = ['host', 'port', 'username', 'password', 'key_file', 'path', 'exclude', 'pre_script', 'post_script']
    MODULES = ['paramiko']

    def __init__(self, row: dict):
        super().__init__(row)
        self.host = (row.get('host') or '').strip()
        # leer = Port aus ~/.ssh/config bzw. 22
        self.port = int(row['port']) if str(row.get('port') or '').strip() else None
        self.username = (row.get('username') or '').strip() or None
        self.password = row.get('password') or None
        self.key_file = (row.get('key_file') or '').strip() or None
        self.client = None
        self.sftp = None
        self.tmp_dir = None

    def validate(self):
        super().validate()
        if self.host == '':
            raise ValueError('Host fehlt')
        if not self.path.startswith('/'):
            raise ValueError('Pfad muss absolut sein, z.B. /home/user/daten')

    def describe(self):
        return f"{self.username + '@' if self.username else ''}{self.host}:{self.path}"

    def ssh_config(self) -> dict:
        """ Eintrag fuer self.host aus ~/.ssh/config (wie ssh), ohne Datei oder paramiko leer """
        path = os.path.expanduser('~/.ssh/config')
        if not os.path.isfile(path):
            return {}
        try:
            import paramiko
            return paramiko.SSHConfig.from_path(path).lookup(self.host)
        except Exception:
            return {}

    def target(self) -> dict:
        """ Verbindungsdaten: Angaben der Quelle, fehlende aus ~/.ssh/config {hostname, port, username, key_files} """
        conf = self.ssh_config()
        if self.key_file:
            key_files = [os.path.expanduser(self.key_file)]
        else:
            key_files = [os.path.expanduser(f) for f in conf.get('identityfile', [])]
        return {'hostname': conf.get('hostname') or self.host,
                'port': self.port if self.port is not None else int(conf.get('port') or 22),
                'username': self.username or conf.get('user'),
                'key_files': key_files}

    def check(self, timeout=3):
        t = self.target()
        try:
            socket.create_connection((t['hostname'], t['port']), timeout=timeout).close()
        except OSError as e:
            alias = f' ({self.host})' if t['hostname'] != self.host else ''
            return False, f"SSH-Host nicht erreichbar: {t['hostname']}:{t['port']}{alias} ({e})"
        return True, self.host

    def open(self):
        import paramiko
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        # unbekannte Host-Schluessel ablehnen statt ungeprueft zu vertrauen
        self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
        t = self.target()
        try:
            # ein key_file der Quelle wird allein verwendet, IdentityFile aus der Config wie bei ssh zusaetzlich zu Agent/~/.ssh
            self.client.connect(t['hostname'], port=t['port'], username=t['username'], password=self.password,
                                key_filename=t['key_files'] or None,
                                look_for_keys=self.key_file is None, allow_agent=self.key_file is None, timeout=10)
        except paramiko.SSHException as e:
            if 'not found in known_hosts' in str(e):
                raise RuntimeError(f'Host-Schluessel von {t["hostname"]} unbekannt, einmal per "ssh -p {t["port"]} '
                                   f'{self.host}" verbinden und bestaetigen ({e})')
            raise
        self.sftp = self.client.open_sftp()
        self.tmp_dir = tempfile.mkdtemp(prefix='nxbackup_ssh_')

    def close(self):
        for closable in (self.sftp, self.client):
            try:
                if closable is not None:
                    closable.close()
            except Exception:
                pass
        self.sftp = self.client = None
        if self.tmp_dir is not None:
            shutil.rmtree(self.tmp_dir, ignore_errors=True)
            self.tmp_dir = None

    def files(self):
        root = self.path.rstrip('/') or '/'

        def walk(remote_dir, rel_dir):
            try:
                entries = self.sftp.listdir_attr(remote_dir)
            except IOError:
                return
            for e in sorted(entries, key=lambda x: x.filename):
                rel = rel_dir + e.filename
                if self.is_excluded(rel):
                    continue
                full = posixpath.join(remote_dir, e.filename)
                if stat_module.S_ISDIR(e.st_mode or 0):
                    yield from walk(full, rel + '/')
                elif stat_module.S_ISREG(e.st_mode or 0):
                    # SFTPAttributes hat st_size und st_mtime wie os.stat
                    yield rel, full, e

        yield from walk(root, '')

    def local_path(self, rel, path, st):
        local = os.path.join(self.tmp_dir, 'file')
        self.sftp.get(path, local)
        # Zeiten der entfernten Datei uebernehmen, die Ziele kopieren sie von der lokalen Datei (copystat, ZIP)
        if st.st_mtime is not None:
            os.utime(local, (st.st_atime or st.st_mtime, st.st_mtime))
        return local

    def release(self, rel, local):
        try:
            os.remove(local)
        except OSError:
            pass

    def run_script(self, script):
        """ auf dem entfernten Rechner ausfuehren """
        stdin, stdout, stderr = self.client.exec_command(script, timeout=self.SCRIPT_TIMEOUT)
        stdin.close()
        output = stdout.read().decode('utf-8', errors='replace') + stderr.read().decode('utf-8', errors='replace')
        return stdout.channel.recv_exit_status(), output


SOURCES = {c.KIND: c for c in [PathSource, FileSource, SshDirSource]}


def parse_patterns(value):
    if not value:
        return []
    return [p.strip() for p in str(value).replace('\n', ',').split(',') if p.strip() != '']


def create_source(row: dict) -> Source:
    kind = row.get('kind')
    if kind not in SOURCES:
        raise ValueError('Unbekannte Quelle: ' + str(kind))
    return SOURCES[kind](row)
