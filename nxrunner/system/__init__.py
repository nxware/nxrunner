import os
import re
import shutil
import subprocess

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import web as w


class Wsl(r.BaseJobExecutor):
    """
      Runner fuer die Ausfuehrung innerhalb der WSL.

      - Windows-Laufwerke (z.B. D:) oder Netzwerkfreigaben (\\\\server\\share) per drvfs mounten
      - Befehle auf der Windows cmd ausfuehren

      op=mount drive=D [mountpoint=/mnt/d]
      op=umount mountpoint=/mnt/d
      op=mounts
      op=drives
      op=volumes
      op=cmd cmd="dir C:\\"
    """

    TAGS = ['system', 'wsl']

    DRIVE_RE = re.compile(r'^([A-Za-z]):?$')

    def __init__(self, type='wsl', timeout=60):
        super().__init__(type)
        self.timeout = int(timeout)
        self.define_vars('timeout')

    @staticmethod
    def is_wsl():
        try:
            with open('/proc/version') as f:
                return 'microsoft' in f.read().lower()
        except OSError:
            return False

    @staticmethod
    def windows_exe(name):
        exe = shutil.which(name + '.exe')
        if exe is None:
            for path in ['/mnt/c/Windows/System32/', '/mnt/c/Windows/System32/WindowsPowerShell/v1.0/']:
                if os.path.exists(path + name + '.exe'):
                    return path + name + '.exe'
        return exe

    @staticmethod
    def windows_cwd():
        # cmd.exe kann nicht in einem Linux-Pfad (UNC) starten
        return '/mnt/c' if os.path.isdir('/mnt/c') else None

    @staticmethod
    def decode(data: bytes):
        for enc in ['utf-8', 'cp850']:
            try:
                return data.decode(enc).replace('\r\n', '\n')
            except UnicodeDecodeError:
                pass
        return data.decode('utf-8', errors='replace').replace('\r\n', '\n')

    def run(self, args, cwd=None, timeout=None):
        try:
            p = subprocess.run(args, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               timeout=timeout or self.timeout)
        except subprocess.TimeoutExpired:
            return self.fail('Timeout: ' + ' '.join(args))
        except OSError as e:
            return self.fail(str(e))
        res = dict(returncode=p.returncode, stdout=self.decode(p.stdout), stderr=self.decode(p.stderr))
        if p.returncode == 0:
            return self.success('ok', **res)
        return self.fail('returncode ' + str(p.returncode), **res)

    def sudo(self, args):
        if os.geteuid() == 0:
            return args
        # -n: nicht nach Passwort fragen, sondern fehlschlagen
        return ['sudo', '-n', *args]

    def source_and_mountpoint(self, data):
        if 'drive' in data and data['drive'] != '':
            m = self.DRIVE_RE.match(data['drive'].strip())
            if m is None:
                raise ValueError('Ungueltiges Laufwerk: ' + data['drive'])
            letter = m.group(1)
            return letter.upper() + ':', data.get('mountpoint') or '/mnt/' + letter.lower()
        elif 'share' in data and data['share'] != '':
            if 'mountpoint' not in data or data['mountpoint'] == '':
                raise ValueError('mountpoint fehlt')
            return data['share'], data['mountpoint']
        raise ValueError('drive oder share fehlt')

    @staticmethod
    def unescape_mount(value):
        # /proc/mounts kodiert Sonderzeichen oktal, z.B. \134 fuer \ und \040 fuer Leerzeichen
        return re.sub(r'\\([0-7]{3})', lambda m: chr(int(m.group(1), 8)), value)

    def mounts(self):
        res = []
        with open('/proc/mounts') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3 and parts[2] in ('drvfs', '9p'):
                    res.append(dict(source=self.unescape_mount(parts[0]), mountpoint=self.unescape_mount(parts[1]), fstype=parts[2]))
        return res

    def execute_mount(self, data):
        try:
            source, mountpoint = self.source_and_mountpoint(data)
        except ValueError as e:
            return self.fail(str(e))
        if os.path.ismount(mountpoint):
            return self.success('bereits gemountet', mountpoint=mountpoint)
        res = self.run(self.sudo(['mkdir', '-p', mountpoint]))
        if not res['success']:
            return res
        # wie das automatische Mounten der WSL (C:, D:): Dateien gehoeren dem Benutzer des Runners, nicht root.
        # Sonst darf er z.B. Zeitstempel und Rechte von Dateien nicht setzen (EPERM).
        options = f'uid={os.getuid()},gid={os.getgid()}'
        res = self.run(self.sudo(['mount', '-t', 'drvfs', source, mountpoint, '-o', options]))
        res['mountpoint'] = mountpoint
        return res

    def execute_umount(self, data):
        mountpoint = data.get('mountpoint', '')
        if mountpoint == '' and data.get('drive', '') != '':
            try:
                mountpoint = self.source_and_mountpoint(data)[1]
            except ValueError as e:
                return self.fail(str(e))
        if not os.path.ismount(mountpoint):
            return self.fail('nicht gemountet: ' + mountpoint)
        return self.run(self.sudo(['umount', mountpoint]))

    def execute_mounts(self, data):
        return self.success('ok', value=self.mounts())

    def execute_drives(self, data):
        ps = self.windows_exe('powershell')
        if ps is None:
            return self.fail('powershell.exe nicht gefunden')
        res = self.run([ps, '-NoProfile', '-Command', '(Get-PSDrive -PSProvider FileSystem).Name'], cwd=self.windows_cwd())
        if res['success']:
            res['value'] = [d.strip() + ':' for d in res['stdout'].splitlines() if d.strip() != '']
        return res

    def volumes(self, timeout=120):
        """
          Windows-Laufwerke mit Namen, z.B. {'D': {'label': 'Daten', 'provider': ''}}
          label ist das Volume-Label, provider bei Netzlaufwerken der Freigabepfad

          Der erste Aufruf kann wegen Netzlaufwerken deutlich laenger dauern (Kaltstart ~1 min).
        """
        ps = self.windows_exe('powershell')
        if ps is None:
            raise RuntimeError('powershell.exe nicht gefunden')
        script = 'Get-CimInstance Win32_LogicalDisk | ForEach-Object { $_.DeviceID + "|" + $_.VolumeName + "|" + $_.ProviderName }'
        res = self.run([ps, '-NoProfile', '-Command', script], cwd=self.windows_cwd(), timeout=timeout)
        if not res['success']:
            raise RuntimeError(res['message'] + ' ' + res.get('stderr', ''))
        volumes = {}
        for line in res['stdout'].splitlines():
            parts = line.strip().split('|', 2)
            if len(parts) == 3 and self.DRIVE_RE.match(parts[0]):
                volumes[parts[0][0].upper()] = dict(label=parts[1].strip(), provider=parts[2].strip())
        return volumes

    def execute_volumes(self, data):
        try:
            return self.success('ok', value=self.volumes())
        except RuntimeError as e:
            return self.fail(str(e))

    def execute_cmd(self, data):
        cmd = self.windows_exe('cmd')
        if cmd is None:
            return self.fail('cmd.exe nicht gefunden')
        if data.get('cmd', '') == '':
            return self.fail('cmd fehlt')
        timeout = int(data['timeout']) if data.get('timeout', '') != '' else None
        return self.run([cmd, '/c', data['cmd']], cwd=self.windows_cwd(), timeout=timeout)

    def part_index(self, p: b.Page, params={}):
        p.h1("WSL")
        p.prop("WSL", self.is_wsl())
        p.prop("cmd.exe", self.windows_exe('cmd'))
        with p.section(h="Mounts"):
            for m in self.mounts():
                p.div(m['source'] + ' &rarr; ' + m['mountpoint'] + ' (' + m['fstype'] + ')')
            p.form_input("drive", "Laufwerk", id='drive', placeholder='D')
            p.form_input("mountpoint", "Mountpoint", id='mountpoint', placeholder='/mnt/d')
            p(self.action_btn_parametric("Mount", dict(type=self.type, op='mount', drive='#drive', mountpoint='#mountpoint')))
            p(self.action_btn_parametric("Umount", dict(type=self.type, op='umount', drive='#drive', mountpoint='#mountpoint')))
            p(self.action_btn_parametric("Laufwerke", dict(type=self.type, op='drives')))
        with p.section(h="cmd"):
            p.form_input("cmd", "Befehl", id='cmd', placeholder='dir C:\\')
            p(self.action_btn_parametric("Ausfuehren", dict(type=self.type, op='cmd', cmd='#cmd')))
        p.pre('', id='result')


# am Ende, da nfile Wsl aus diesem Modul importiert
from nxrunner.system.nfile import NFile  # noqa: E402
from nxrunner.system.backup import Backup  # noqa: E402
