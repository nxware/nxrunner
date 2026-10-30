import os
import shutil
import socket
import tempfile


class Destination:
    """
      Ziel eines Backups. Die Dateien einer Quelle landen unter <Ziel>/<Name der Quelle>/<relativer Pfad>.
      Ablauf: open(source), copy(...) je Datei, commit() wenn der Lauf erfolgreich war, close() immer
      Neue Arten: Unterklasse mit KIND anlegen und in DESTINATIONS eintragen.
    """

    KIND = None
    TITLE = ''
    # Spalten aus backup_destination, die diese Art verwendet
    FIELDS = []
    # True: unveraenderte Dateien werden uebersprungen; False: jeder Lauf schreibt alle Dateien (z.B. ZIP)
    INCREMENTAL = True

    def __init__(self, row: dict, backup=None):
        self.id = row.get('destination_id')
        self.name = row.get('name') or ''
        self.path = (row.get('path') or '').strip()
        self.row = row
        self.backup = backup

    def validate(self):
        if self.name.strip() == '':
            raise ValueError('Name fehlt')

    def describe(self):
        return self.path

    def check(self, timeout=3):
        """
          Ziele sind nicht immer erreichbar (USB-Platte abgesteckt, NAS aus).
          :return: (erreichbar, Meldung)
        """
        return True, 'ok'

    def open(self, source):
        pass

    def copy(self, rel, full, st):
        raise NotImplementedError()

    def usage(self):
        """ (Gesamtgroesse, belegt) des Mediums in Bytes oder None, wenn nicht ermittelbar """
        return None

    def has(self, rel, size):
        """
          Liegt die Datei mit dieser Groesse noch im Ziel? Unveraenderte Dateien werden nur uebersprungen, wenn
          ja, damit z.B. im Ziel geloeschte Dateien erneut gesichert werden. Standard: den Metadaten vertrauen.
        """
        return True

    def commit(self):
        """ Lauf erfolgreich: Sicherung abschliessen (z.B. ZIP fertigstellen) """
        pass

    def close(self):
        """ Aufraeumen, auch nach Fehlern; ohne commit() angefangene Sicherungen verwerfen """
        pass


class DriveDestination(Destination):
    """ Lokales Medium: Laufwerksname (wie in nfile) und Pfad auf dem Medium """

    KIND = 'drive'
    TITLE = 'Laufwerk'
    FIELDS = ['drive', 'path']

    def __init__(self, row: dict, backup=None):
        super().__init__(row, backup)
        self.drive = (row.get('drive') or '').strip()
        self.root = None
        self.created = set()

    def validate(self):
        super().validate()
        if self.drive == '':
            raise ValueError('Laufwerk fehlt')

    def describe(self):
        return self.drive + ':/' + self.path.strip('/')

    def check(self, timeout=3):
        mountpoint = self.backup.drive_mountpoint(self.drive)
        if mountpoint is None:
            return False, 'Laufwerk nicht gemountet: ' + self.drive
        try:
            os.listdir(mountpoint)
        except OSError as e:
            return False, f'Laufwerk nicht lesbar: {self.drive} ({e})'
        return True, mountpoint

    def usage(self):
        mountpoint = self.backup.drive_mountpoint(self.drive)
        if mountpoint is None:
            return None
        try:
            du = shutil.disk_usage(mountpoint)
            return du.total, du.used
        except OSError:
            return None

    def open(self, source):
        mountpoint = self.backup.drive_mountpoint(self.drive)
        if mountpoint is None:
            raise RuntimeError('Laufwerk nicht gemountet: ' + self.drive)
        base = os.path.realpath(mountpoint)
        root = os.path.realpath(os.path.join(base, self.path.strip('/'), source.name))
        if not root.startswith(base + os.sep):
            raise RuntimeError('Pfad ausserhalb des Laufwerks: ' + self.path)
        self.root = root
        self.created = set()
        self.listing = {}

    def has(self, rel, size):
        # je Verzeichnis einmal einlesen: {Name: Groesse}
        parent, name = os.path.split(os.path.join(self.root, *rel.split('/')))
        if parent not in self.listing:
            try:
                self.listing[parent] = {e.name: e.stat().st_size for e in os.scandir(parent) if e.is_file()}
            except OSError:
                self.listing[parent] = {}
        return self.listing[parent].get(name) == size

    def copy(self, rel, full, st):
        target = os.path.join(self.root, *rel.split('/'))
        parent = os.path.dirname(target)
        if parent not in self.created:
            os.makedirs(parent, exist_ok=True)
            self.created.add(parent)
        # erst vollstaendig schreiben, dann ersetzen, damit keine halben Dateien entstehen
        tmp = target + '.nxbackup.tmp'
        try:
            shutil.copyfile(full, tmp)
            try:
                shutil.copystat(full, tmp)
            except OSError:
                # Zeitstempel/Rechte duerfen nicht immer gesetzt werden (z.B. drvfs-Mount ohne uid, Dateien gehoeren
                # root -> EPERM). Der Inhalt ist gesichert, inkrementell wird mit den Daten der Quelle verglichen.
                pass
            os.replace(tmp, target)
        except BaseException:
            # keine halbe temporaere Datei zuruecklassen
            try:
                os.remove(tmp)
            except OSError:
                pass
            raise


class DriveDestinationZip(DriveDestination):
    """
      Lokales Medium, gesichert wird in eine ZIP-Datei <Pfad>/<Name der Quelle>.zip.
      Jeder Lauf schreibt die ZIP komplett neu (erst als temporaere Datei) und ersetzt danach die alte.
      keep > 0: so viele vorige Versionen behalten (<Name>.1.zip ist die vorige, <Name>.2.zip die davor ...).
    """

    KIND = 'drive_zip'
    TITLE = 'Laufwerk (ZIP)'
    FIELDS = ['drive', 'path', 'keep']
    INCREMENTAL = False
    SUFFIX = '.zip'

    def __init__(self, row: dict, backup=None):
        super().__init__(row, backup)
        self.keep = max(0, int(row.get('keep') or 0))
        self.zip = None
        self.target = None
        self.tmp = None

    def describe(self):
        text = self.drive + ':/' + (self.path.strip('/') + '/' if self.path.strip('/') else '') + '<Quelle>' + self.SUFFIX
        return text + (f', {self.keep} alte Versionen' if self.keep else '')

    def open(self, source):
        super().open(source)
        # super().open liefert <Pfad>/<Quelle> als Verzeichnis, hier wird daraus <Pfad>/<Quelle>.zip
        self.target = self.root + self.SUFFIX
        os.makedirs(os.path.dirname(self.target), exist_ok=True)
        self.tmp = self.target + '.nxbackup.tmp'
        self.zip = self.open_zip(self.tmp)

    @staticmethod
    def open_zip(path):
        import zipfile
        return zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED, allowZip64=True)

    def copy(self, rel, full, st):
        self.zip.write(full, arcname=rel)

    def version(self, n):
        return self.target[:-len(self.SUFFIX)] + f'.{n}' + self.SUFFIX

    def rotate(self):
        """ vorige Versionen weiterschieben, die aelteste ueber keep hinaus entfaellt """
        if self.keep == 0 or not os.path.exists(self.target):
            return
        if os.path.exists(self.version(self.keep)):
            os.remove(self.version(self.keep))
        for n in range(self.keep - 1, 0, -1):
            if os.path.exists(self.version(n)):
                os.replace(self.version(n), self.version(n + 1))
        os.replace(self.target, self.version(1))

    def commit(self):
        self.zip.close()
        self.zip = None
        self.rotate()
        os.replace(self.tmp, self.target)
        self.tmp = None

    def close(self):
        # ohne commit (Fehler, Abbruch): angefangene ZIP verwerfen, die bisherige bleibt erhalten
        if self.zip is not None:
            try:
                self.zip.close()
            except Exception:
                pass
            self.zip = None
        if self.tmp is not None and os.path.exists(self.tmp):
            try:
                os.remove(self.tmp)
            except OSError:
                pass
        self.tmp = None


class SmbDestination(Destination):
    """ SMB-Freigabe (\\\\host\\share\\pfad) ueber smbprotocol/smbclient """

    KIND = 'smb'
    TITLE = 'SMB'
    FIELDS = ['host', 'share', 'path', 'username', 'password']

    def __init__(self, row: dict, backup=None):
        super().__init__(row, backup)
        self.host = (row.get('host') or '').strip()
        self.share = (row.get('share') or '').strip().strip('/\\')
        self.username = row.get('username') or ''
        self.password = row.get('password') or ''
        self.smb = None
        self.base = None
        self.created = set()

    def validate(self):
        super().validate()
        if self.host == '' or self.share == '':
            raise ValueError('Host und Freigabe fehlen')

    def describe(self):
        return f"\\\\{self.host}\\{self.share}\\" + self.path.strip('/\\').replace('/', '\\')

    def check(self, timeout=3):
        try:
            socket.create_connection((self.host, 445), timeout=timeout).close()
        except OSError as e:
            return False, f'SMB-Host nicht erreichbar: {self.host} ({e})'
        return True, self.host

    def open(self, source):
        from nxrunner.fs import Smb
        self.smb = Smb()
        self.smb.connect(self.host, self.username, self.password)
        parts = [self.share, *[p for p in self.path.replace('\\', '/').split('/') if p not in ('', '.', '..')], source.name]
        self.base = '/' + '/'.join(parts)
        self.created = set()
        self.listing = {}

    def has(self, rel, size):
        # je Verzeichnis einmal einlesen (eine Netzwerkanfrage statt einer je Datei): {Name: Groesse}
        parent, name = (self.base + '/' + rel).rsplit('/', 1)
        if parent not in self.listing:
            try:
                self.listing[parent] = {e.name: e.stat().st_size for e in self.smb.client.scandir(self.smb._build_path(parent))
                                        if e.is_file()}
            except Exception:
                self.listing[parent] = {}
        return self.listing[parent].get(name) == size

    def makedirs(self, path):
        if path not in self.created:
            self.smb.makedirs(path)
            self.created.add(path)

    def copy(self, rel, full, st):
        target = self.base + '/' + rel
        self.makedirs(target.rsplit('/', 1)[0])
        self.smb.copy_to_remote(full, target, mtime=st.st_mtime)

    def close(self):
        self.smb = None


class ZipCryptMixin:
    """
      ZIP-Sicherung, verschluesselt mit dem CryptRunner crypt (op encrypt_stream_file, AES-256-GCM, .zip.nxc):
      Die ZIP entsteht lokal in einer temporaeren Datei, wird bei commit() verschluesselt und die verschluesselte
      Datei dann ins Ziel geschoben. Lokale Zwischendateien werden immer geloescht (auch nach Fehlern).
      Entschluesseln z.B. mit op=decrypt_stream_file oder dem Upload-Formular des crypt-Runners.
    """

    SUFFIX = '.zip.nxc'
    local_zip = None
    local_enc = None

    def crypt_executor(self):
        root = self.backup.getRoot()
        executor = getattr(root, 'jobexecutor', root)
        if 'crypt' not in executor:
            raise RuntimeError('CryptRunner crypt nicht verfuegbar, verschluesselte Sicherung nicht moeglich')
        return executor

    def open_local_zip(self):
        self.crypt_executor()  # vor dem Packen pruefen, nicht erst beim commit
        fd, self.local_zip = tempfile.mkstemp(prefix='nxbackup_', suffix='.zip')
        os.close(fd)
        self.local_enc = None
        self.zip = DriveDestinationZip.open_zip(self.local_zip)

    def encrypt_local(self):
        """ ZIP abschliessen und verschluesseln, :return: Pfad der lokalen verschluesselten Datei """
        self.zip.close()
        self.zip = None
        self.local_enc = self.local_zip + '.nxc'
        res = self.crypt_executor().execute(dict(type='crypt', op='encrypt_stream_file', infile=self.local_zip,
                                                 outfile=self.local_enc, overwrite=True))
        self.remove_local()
        if not res.get('success'):
            raise RuntimeError('Verschluesselung fehlgeschlagen: ' + str(res.get('message')))
        return self.local_enc

    def remove_local(self, enc=False):
        """ lokale ZIP loeschen, mit enc=True auch die verschluesselte Datei """
        for name in (['local_zip', 'local_enc'] if enc else ['local_zip']):
            path = getattr(self, name)
            if path is not None and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
            setattr(self, name, None)

    def close_local(self):
        if self.zip is not None:
            try:
                self.zip.close()
            except Exception:
                pass
            self.zip = None
        self.remove_local(enc=True)


class DriveDestinationZipCrypt(ZipCryptMixin, DriveDestinationZip):
    """
      Wie DriveDestinationZip, aber verschluesselt: <Pfad>/<Name der Quelle>.zip.nxc (alte Versionen <Name>.1.zip.nxc ...).
      Die ZIP entsteht lokal, auf das Laufwerk kommt nur die verschluesselte Datei.
    """

    KIND = 'drive_zip_crypt'
    TITLE = 'Laufwerk (ZIP, verschlüsselt)'

    def open(self, source):
        DriveDestination.open(self, source)
        self.target = self.root + self.SUFFIX
        os.makedirs(os.path.dirname(self.target), exist_ok=True)
        self.tmp = self.target + '.nxbackup.tmp'
        self.open_local_zip()

    def commit(self):
        local = self.encrypt_local()
        shutil.copyfile(local, self.tmp)
        self.remove_local(enc=True)
        self.rotate()
        os.replace(self.tmp, self.target)
        self.tmp = None

    def close(self):
        self.close_local()
        super().close()


class SmbDestinationZipCrypt(ZipCryptMixin, SmbDestination):
    """
      SMB-Freigabe, gesichert wird verschluesselt nach \\\\host\\share\\pfad\\<Name der Quelle>.zip.nxc.
      Die ZIP entsteht lokal, hochgeladen wird nur die verschluesselte Datei (erst als .nxbackup.tmp, dann
      umbenannt, damit eine halbe Sicherung nicht die vorige ersetzt).
    """

    KIND = 'smb_zip_crypt'
    TITLE = 'SMB (ZIP, verschlüsselt)'
    INCREMENTAL = False

    def __init__(self, row: dict, backup=None):
        super().__init__(row, backup)
        self.zip = None
        self.target = None

    def describe(self):
        return super().describe().rstrip('\\') + '\\<Quelle>' + self.SUFFIX

    def open(self, source):
        super().open(source)
        # super().open liefert /share/pfad/<Quelle> als Verzeichnis, hier wird daraus /share/pfad/<Quelle>.zip.nxc
        self.target = self.base + self.SUFFIX
        self.open_local_zip()

    def copy(self, rel, full, st):
        self.zip.write(full, arcname=rel)

    def commit(self):
        local = self.encrypt_local()
        self.makedirs(self.target.rsplit('/', 1)[0])
        tmp = self.smb._build_path(self.target + '.nxbackup.tmp')
        try:
            self.smb.copy_to_remote(local, self.target + '.nxbackup.tmp')
            self.remove_local(enc=True)
            self.smb.client.replace(tmp, self.smb._build_path(self.target))
        except BaseException:
            try:
                self.smb.client.remove(tmp)
            except Exception:
                pass
            raise

    def close(self):
        self.close_local()
        super().close()


DESTINATIONS = {c.KIND: c for c in [DriveDestination, DriveDestinationZip, DriveDestinationZipCrypt, SmbDestination,
                                    SmbDestinationZipCrypt]}


def create_destination(row: dict, backup=None) -> Destination:
    kind = row.get('kind')
    if kind not in DESTINATIONS:
        raise ValueError('Unbekanntes Ziel: ' + str(kind))
    return DESTINATIONS[kind](row, backup)
