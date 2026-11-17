"""
Videos aus einem Verzeichnis anzeigen, beschneiden und komprimiert herunterladen.

Videos, zeigt die Videos eines Verzeichnisses:
    Navigation: Index (Videos im Hauptverzeichnis), Browse (Verzeichnisse, werden erst beim Oeffnen gelesen), Videos (Video-Runner mit Upload)
    Ein ausgewaehltes Video wird im Video-Runner geoeffnet. Fehlt dieser im Dispatcher, wird er angelegt.

    Config (runners):
        videos: nxrunner.app.videos:Videos(path="~/Videos")
        videos: nxrunner.app.videos:Videos(path="/srv/videos", video="video")

Video, Editor fuer ein einzelnes Video:
    Ohne Video (Parameter file) wird ein Upload angeboten (nur per Formular direkt auf den Runner,
    nicht ueber NWebClient), hochgeladene Videos landen lokal in path.
    Mit Video: Vorschau, Start/Ende setzen und mit ffmpeg (H.264/AAC, MP4) beschnitten und
    komprimiert herunterladen. Erlaubt sind nur Dateien in path und in den Verzeichnissen der Videos-Runner.

    Config (runners):
        video: nxrunner.app.videos:Video()
        video: nxrunner.app.videos:Video(path="~/.cache/nxrunner/videos")

    Jobs:
        {"type": "video", "op": "compress", "file": "/srv/videos/a.mp4", "start": 5, "end": 20, "quality": "medium"}
        {"type": "video", "op": "compress", "file": "/srv/videos/a.mp4", "quality": "low", "bitrate": "800k"}
        {"type": "video", "op": "clean_files"}   loescht alles in path (Uploads, Exporte), path daher nur als Cache nutzen

    Benoetigt ffmpeg im PATH.
"""
import os
import re
import shutil
import subprocess
import tempfile
import time
import mimetypes
import urllib.parse
from io import BytesIO
from pathlib import Path

from nwebclient import runner as r, base
from nwebclient.runner import op
from nwebclient import web as w


VIDEO_EXTS = ['mp4', 'm4v', 'mov', 'mkv', 'webm', 'avi', 'wmv', 'mpg', 'mpeg', 'ts', '3gp']

# Qualitaet -> (CRF, max. Hoehe in Pixel, Audio-Bitrate)
QUALITIES = {
    'high': (23, 1080, '160k'),
    'medium': (28, 720, '128k'),
    'low': (32, 480, '96k'),
}

QUALITY_TITLES = {'high': "Hoch (1080p)", 'medium': "Mittel (720p)", 'low': "Niedrig (480p)"}

# Video-Bitrate, '' = automatisch ueber CRF der Qualitaet (konstante Qualitaet, Groesse haengt vom Inhalt ab)
BITRATES = {
    '': "Automatisch (CRF)",
    '4000k': "4 Mbit/s",
    '2500k': "2,5 Mbit/s",
    '1500k': "1,5 Mbit/s",
    '800k': "800 kbit/s",
    '400k': "400 kbit/s",
}


def is_video_file(path: str) -> bool:
    return os.path.isfile(path) and path.rsplit('.', 1)[-1].lower() in VIDEO_EXTS


def format_size(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f'{size:.0f} {unit}'
        size /= 1024
    return f'{size:.1f} TB'


def parse_time(value) -> float:
    """
    Sekunden aus "12.5", "1:30" oder "01:02:03.5", leer = 0
    """
    value = str(value or '').strip()
    if value == '':
        return 0.0
    seconds = 0.0
    for part in value.split(':'):
        seconds = seconds * 60 + float(part.replace(',', '.'))
    return max(0.0, seconds)


def safe_filename(name: str) -> str:
    name = os.path.basename(str(name or '').replace('\\', '/'))
    name = re.sub(r'[^\w.\- ]', '_', name).strip(' .')
    return name or 'video.mp4'


def video_rate_args(crf: int, bitrate: str) -> list:
    """ CRF (konstante Qualitaet) oder feste Ziel-Bitrate aus BITRATES """
    if bitrate and bitrate in BITRATES:
        kbit = int(bitrate.rstrip('k'))
        return ['-b:v', bitrate, '-maxrate', bitrate, '-bufsize', f'{2 * kbit}k']
    return ['-crf', str(crf)]


def ffmpeg_args(src: str, dst: str, start: float, end: float, quality: str, bitrate: str = '') -> list:
    crf, height, audio = QUALITIES.get(quality, QUALITIES['medium'])
    args = ['ffmpeg', '-y', '-hide_banner', '-loglevel', 'error']
    if start > 0:
        args += ['-ss', f'{start:.3f}']
    args += ['-i', src]
    if end > start:
        args += ['-t', f'{end - start:.3f}']
    args += ['-vf', f"scale=-2:'min({height},ih)'", '-c:v', 'libx264', '-preset', 'veryfast',
             *video_rate_args(crf, bitrate),
             '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-b:a', audio, '-movflags', '+faststart', dst]
    return args


def export_name(src: str, quality: str, bitrate: str = '') -> str:
    return Path(src).stem + '_' + quality + ('_' + bitrate if bitrate in BITRATES and bitrate else '') + '.mp4'


class Video(r.BaseJobExecutor):
    """
    Video-Editor: Upload, Vorschau, Beschneiden und komprimierter Download (ffmpeg).
    """

    def __init__(self, path: str = '~/.cache/nxrunner/videos', type: str = 'video'):
        super().__init__(type)
        self.path = os.path.realpath(os.path.expanduser(path))
        self.allowed_dirs = [self.path]

    def allow(self, path: str):
        """ Videos in path (und Unterverzeichnissen) duerfen geoeffnet werden """
        path = os.path.realpath(os.path.expanduser(path))
        if path not in self.allowed_dirs:
            self.allowed_dirs.append(path)

    def resolve(self, file) -> str:
        """ :return: absoluter Pfad, wenn das Video in einem erlaubten Verzeichnis liegt, sonst None """
        if not file:
            return None
        path = os.path.realpath(os.path.expanduser(str(file)))
        for d in self.allowed_dirs + self.videos_dirs():
            if (path == d or path.startswith(d.rstrip(os.sep) + os.sep)) and is_video_file(path):
                return path
        return None

    def videos_dirs(self) -> list:
        """ Verzeichnisse der geladenen Videos-Runner im Dispatcher (auch ohne vorherigen Aufruf von Videos) """
        d = self.getParentClass(r.LazyDispatcher)
        if d is None:
            return []
        return [runner.path for runner in d.loaded() if isinstance(runner, Videos)]

    def link_video(self, path: str) -> str:
        return self.link(self.part_index, file=path)

    # --- Operationen --------------------------------------------------------------------------

    def compress_file(self, src: str, dst: str, start: float, end: float, quality: str, bitrate: str = ''):
        """ :return: None oder Fehlermeldung """
        if shutil.which('ffmpeg') is None:
            return 'ffmpeg nicht gefunden'
        res = subprocess.run(ffmpeg_args(src, dst, start, end, quality, bitrate), capture_output=True, text=True)
        if res.returncode != 0:
            return (res.stderr or 'ffmpeg Fehler').strip()[-2000:]
        return None

    @op("Beschneidet ein Video (start/end in s, end=0 bis Ende) und komprimiert es als MP4, quality: high|medium|low, "
        "bitrate: Video-Bitrate (4000k|2500k|1500k|800k|400k, leer = automatisch per CRF)",
        result={'success': 'bool', 'message': 'str', 'file': 'str'})
    def compress(self, file: str, start: float = 0, end: float = 0, quality: str = 'medium', bitrate: str = '',
                 out: str = ''):
        src = self.resolve(file)
        if src is None:
            return self.fail('Video nicht gefunden oder nicht erlaubt')
        if bitrate not in BITRATES:
            return self.fail('Unbekannte Bitrate: ' + str(bitrate))
        if out == '':
            out = os.path.join(self.path, 'exports', export_name(src, quality, bitrate))
        os.makedirs(os.path.dirname(out), exist_ok=True)
        error = self.compress_file(src, out, parse_time(start), parse_time(end), quality, bitrate)
        if error:
            return self.fail(error)
        return self.success('Komprimiert', file=out, size=os.path.getsize(out))

    def cache_files(self) -> list:
        """ Dateien im Cache: Uploads und Exporte in path sowie liegengebliebene Temp-Verzeichnisse von download """
        files = []
        if os.path.isdir(self.path):
            for root, dirs, names in os.walk(self.path):
                files += [os.path.join(root, n) for n in names]
        tmp = tempfile.gettempdir()
        files += [os.path.join(tmp, n) for n in os.listdir(tmp) if n.startswith('nxvideo_')]
        return files

    @op("Loescht den Cache: hochgeladene und exportierte Videos in path sowie Temp-Dateien",
        result={'success': 'bool', 'message': 'str', 'count': 'int', 'size': 'int'})
    def clean_files(self):
        count, size, errors = 0, 0, []
        for f in self.cache_files():
            try:
                if os.path.isdir(f):
                    shutil.rmtree(f)
                else:
                    file_size = os.path.getsize(f)
                    os.remove(f)
                    size += file_size
                count += 1
            except OSError as e:
                errors.append(f'{f}: {e}')
        if os.path.isdir(os.path.join(self.path, 'exports')):
            shutil.rmtree(os.path.join(self.path, 'exports'), ignore_errors=True)
        message = f'{count} Dateien gelöscht ({format_size(size)})'
        if errors:
            return self.fail(message + ', Fehler: ' + '; '.join(errors), count=count, size=size)
        return self.success(message, count=count, size=size, ui_reload=True)

    def unique_path(self, name: str) -> str:
        os.makedirs(self.path, exist_ok=True)
        stem, ext = os.path.splitext(name)
        dst, i = os.path.join(self.path, name), 1
        while os.path.exists(dst):
            dst, i = os.path.join(self.path, f'{stem}_{i}{ext}'), i + 1
        return dst

    # --- Seiten -------------------------------------------------------------------------------

    def page_upload(self, params={}):
        """ Nimmt den Formular-Upload (Feld upload) entgegen und leitet in den Editor weiter """
        from flask import redirect
        f = params.get('upload')
        if f is None or not getattr(f, 'filename', ''):
            return redirect('/pysys/dispatcher' + self.link(self.part_index, error="Keine Datei ausgewaehlt"))
        name = safe_filename(f.filename)
        if name.rsplit('.', 1)[-1].lower() not in VIDEO_EXTS:
            return redirect('/pysys/dispatcher' + self.link(self.part_index, error="Kein Videoformat: " + name))
        dst = self.unique_path(name)
        f.save(dst)
        return redirect('/pysys/dispatcher' + self.link_video(dst))

    def page_stream(self, params={}):
        """ Liefert das Video fuer die Vorschau (mit Range-Requests zum Spulen) """
        from flask import send_file
        path = self.resolve(params.get('file'))
        if path is None:
            return "404, Video nicht gefunden oder nicht erlaubt", 404
        return send_file(path, mimetype=mimetypes.guess_type(path)[0] or 'video/mp4', conditional=True)

    THUMB_WIDTH = 240

    def thumbnail(self, path: str):
        """
        Vorschaubild (JPEG) aus dem Video, wird beim ersten Abruf mit ffmpeg erzeugt und in path/thumbs gecacht
        :return: Pfad des Bildes oder None
        """
        import hashlib
        st = os.stat(path)
        key = hashlib.sha1(f'{path}|{st.st_size}|{st.st_mtime}'.encode('utf-8')).hexdigest()
        thumb = os.path.join(self.path, 'thumbs', key + '.jpg')
        if os.path.isfile(thumb):
            return thumb
        if shutil.which('ffmpeg') is None:
            self.error('Thumbnail: ffmpeg nicht gefunden')
            return None
        os.makedirs(os.path.dirname(thumb), exist_ok=True)
        for seek in ['1', '0']:  # sehr kurze Videos haben bei 1 s kein Bild mehr
            res = subprocess.run(['ffmpeg', '-y', '-hide_banner', '-loglevel', 'error', '-ss', seek, '-i', path,
                                  '-frames:v', '1', '-vf', f'scale={self.THUMB_WIDTH}:-2', thumb],
                                 capture_output=True, text=True)
            if os.path.isfile(thumb) and os.path.getsize(thumb) > 0:
                return thumb
        self.error(f'Thumbnail fehlgeschlagen: {path}: {res.stderr.strip()[-500:]}')
        return None

    def page_thumb(self, params={}):
        """ Liefert das Vorschaubild eines Videos (Parameter file) """
        from flask import send_file
        path = self.resolve(params.get('file'))
        thumb = self.thumbnail(path) if path is not None else None
        if thumb is None:
            return "404, kein Vorschaubild", 404
        return send_file(thumb, mimetype='image/jpeg', max_age=86400)

    def page_download(self, params={}):
        """ Beschneidet und komprimiert das Video und liefert es als Download """
        from flask import send_file
        path = self.resolve(params.get('file'))
        if path is None:
            return self.page_error("Video nicht gefunden oder nicht erlaubt")
        quality = params.get('quality', 'medium')
        bitrate = params.get('bitrate', '')
        bitrate = bitrate if bitrate in BITRATES else ''
        tmp = tempfile.mkdtemp(prefix='nxvideo_')
        try:
            dst = os.path.join(tmp, export_name(path, quality, bitrate))
            error = self.compress_file(path, dst, parse_time(params.get('start')), parse_time(params.get('end')),
                                       quality, bitrate)
            if error:
                return self.page_error(error, path)
            with open(dst, 'rb') as f:
                data = BytesIO(f.read())
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        return send_file(data, mimetype='video/mp4', as_attachment=True, download_name=os.path.basename(dst))

    def page_error(self, message: str, path: str = None):
        p = base.Page(owner=self)
        p.h1("Fehler")
        p.pre(w.s(message))
        p.div(w.a("Zurück", self.link_video(path) if path else self.link(self.part_index), hint="Zurück zum Editor"))
        return p.nxui()

    def part_index(self, p: base.Page, params={}):
        if params.get('error'):
            p.div(w.s(params['error']), style='color: #c33;')
        path = self.resolve(params.get('file'))
        if params.get('file') and path is None:
            p.div("Video nicht gefunden oder nicht erlaubt: " + w.s(params['file']), style='color: #c33;')
        if path is None:
            self.part_upload(p, params)
        else:
            self.part_editor(p, path)

    def part_upload(self, p: base.Page, params={}):
        p.h2("Video hochladen")
        p("Ein Video hochladen, um es danach zu beschneiden und komprimiert herunterzuladen.")
        action = '/pysys/dispatcher' + self.link(self.page_upload)
        with p.t('form', method='post', enctype='multipart/form-data', action=w.s(action)):
            p.input('upload', hint="Videodatei auswählen", type='file', accept='video/*')
            p.input('submit', hint="Video hochladen und im Editor öffnen", type='submit', value="Hochladen")
        uploads = self.uploaded_videos()
        if uploads:
            with p.section(h="Hochgeladene Videos"):
                for f in uploads:
                    p.div(w.a(w.s(os.path.basename(f)), self.link_video(f), hint=f))
        self.part_cache(p)

    def part_cache(self, p: base.Page):
        files = [f for f in self.cache_files() if os.path.isfile(f)]
        size = sum(os.path.getsize(f) for f in files)
        with p.section(h="Cache"):
            p.prop("Verzeichnis", w.s(self.path))
            p.prop("Belegt", f'{len(files)} Dateien, {format_size(size)}')
            p(self.action_btn({'type': self.type, 'op': 'clean_files'}, label="Cache leeren",
                              hint="Alle hochgeladenen und exportierten Videos im Cache-Verzeichnis löschen"))

    def uploaded_videos(self) -> list:
        if not os.path.isdir(self.path):
            return []
        files = [os.path.join(self.path, n) for n in os.listdir(self.path)]
        return sorted([f for f in files if is_video_file(f)], key=os.path.getmtime, reverse=True)

    def part_editor(self, p: base.Page, path: str):
        p.h2(w.s(os.path.basename(path)))
        p.prop("Datei", w.s(path))
        p.prop("Größe", format_size(os.path.getsize(path)))
        stream = '/pysys/dispatcher' + self.link(self.page_stream, file=path)
        p('<div style="display: inline-block; max-width: 100%; vertical-align: top;">')  # Leiste so breit wie das Video
        p(f'<video id="nxvideo" src="{w.s(stream)}" controls preload="metadata" '
          f'style="max-width: 100%; max-height: 60vh; display: block; background: #000;"></video>')
        self.part_cut_slider(p)
        p('</div>')
        self.part_cut_form(p, path)
        self.part_send(p, path)
        self.part_cut_js(p)
        self.part_slider_js(p)

    # --- An Scene/Timeline senden -------------------------------------------------------------

    def send_targets(self) -> dict:
        """ {Ziel: Titel}: geladene Timelines ('tl:<type>') und Scenes ('scene:<type>' = neue Timeline darin) """
        from nxrunner.animate import Timeline, Scene
        d = self.getParentClass(r.LazyDispatcher)
        if d is None:
            return {}
        runners = d.loaded()
        scene_of = {tl.type: s.type for s in runners if isinstance(s, Scene) for tl in s.items}
        res = {}
        for runner in runners:
            if isinstance(runner, Timeline):
                scene = scene_of.get(runner.type)
                res['tl:' + runner.type] = (scene + ' / ' if scene else '') + runner.type
        for runner in runners:
            if isinstance(runner, Scene):
                res['scene:' + runner.type] = runner.type + ' / neue Timeline'
        return res

    def find_target_timeline(self, target: str):
        """ :return: (Timeline, None) oder (None, Fehlermeldung); scene:<type> legt eine neue Timeline an """
        from nxrunner.animate import Timeline, Scene
        d = self.getParentClass(r.LazyDispatcher)
        kind, _, name = str(target or '').partition(':')
        runner = d.get_runner(name) if d is not None and name else None
        if kind == 'tl' and isinstance(runner, Timeline):
            return runner, None
        if kind == 'scene' and isinstance(runner, Scene):
            return runner.get_timeline(runner.add_timeline().get('name')), None
        return None, 'Ziel nicht gefunden: ' + str(target)

    @op("Video-Abschnitt als Clip (Eintrag einer Timeline): file, start/end in s",
        result={'success': 'bool', 'file': 'str', 'start': 'float', 'end': 'float', 'duration': 'float'})
    def clip(self, file: str, start: float = 0, end: float = 0):
        src = self.resolve(file)
        if src is None:
            return self.fail('Video nicht gefunden oder nicht erlaubt')
        start, end = parse_time(start), parse_time(end)
        return self.success('Clip ' + os.path.basename(src), file=src, start=start, end=end,
                            duration=max(0.0, end - start) if end > start else None)

    @op("Haengt den Video-Abschnitt als Clip an eine Timeline an (target: tl:<timeline> oder scene:<scene> fuer eine "
        "neue Timeline), at = Startzeit in der Timeline in s, leer = am Ende",
        result={'success': 'bool', 'message': 'str', 'timeline': 'str', 'guid': 'str'})
    def send_to_timeline(self, target: str, file: str, start: float = 0, end: float = 0, at: str = ''):
        src = self.resolve(file)
        if src is None:
            return self.fail('Video nicht gefunden oder nicht erlaubt')
        timeline, error = self.find_target_timeline(target)
        if error:
            return self.fail(error)
        start, end = parse_time(start), parse_time(end)
        job = '?' + urllib.parse.urlencode({'type': self.type, 'op': 'clip', 'file': src,
                                            'start': f'{start:g}', 'end': f'{end:g}'})
        item = {'job': job, 'duration': end - start if end > start else None}
        if str(at).strip() != '':
            item['start'] = parse_time(at)
        res = timeline.add_item(item)
        return self.success(f'An {timeline.type} gesendet', timeline=timeline.type, guid=res.get('guid'))

    def part_send(self, p: base.Page, path: str):
        targets = self.send_targets()
        with p.section(h="An Scene/Timeline senden"):
            if not targets:
                p.div("Keine Scene oder Timeline geladen (Runner nxrunner.animate:Scene bzw. :Timeline)",
                      style='opacity: .65;')
                return
            p.form_select('target', "Ziel", targets, id='nxsend_target',
                          hint="Timeline, an die der Abschnitt angehängt wird, oder neue Timeline einer Scene")
            p.form_input('at', "Startzeit (s)", id='nxsend_at', size='8',
                         hint="Position in der Timeline, leer = am Ende anhängen")
            data = {'type': self.type, 'op': 'send_to_timeline', 'file': path, 'target': '#nxsend_target',
                    'at': '#nxsend_at', 'start': '#nxvideo_start', 'end': '#nxvideo_end'}
            p(self.action_btn_parametric("Senden", data,
                                         hint="Ausgewählten Abschnitt (Start bis Ende) als Clip in die Timeline einfügen"))
            p.div('', id='result', style='display: none;')  # exec_job_p schreibt hier JSON hinein, Meldung kommt per show_result

    SLIDER_CSS = (
        ".nxcut { position: relative; height: 34px; margin: 10px 0 4px; max-width: 100%; }"
        " .nxcut_track { position: absolute; left: 0; right: 0; top: 13px; height: 8px; border-radius: 4px;"
        " background: rgba(128,128,128,.30); }"
        " .nxcut_sel { position: absolute; top: 13px; height: 8px; border-radius: 4px; background: #2a7fd4; }"
        " .nxcut_head { position: absolute; top: 6px; width: 2px; height: 22px; background: #e33; pointer-events: none; }"
        " .nxcut input[type=range] { position: absolute; left: 0; top: 7px; width: 100%; margin: 0; height: 20px;"
        " background: none; pointer-events: none; -webkit-appearance: none; appearance: none; }"
        " .nxcut input[type=range]::-webkit-slider-thumb { pointer-events: auto; -webkit-appearance: none;"
        " width: 14px; height: 22px; border-radius: 3px; background: #fff; border: 2px solid #2a7fd4; cursor: ew-resize; }"
        " .nxcut input[type=range]::-moz-range-thumb { pointer-events: auto; width: 12px; height: 20px;"
        " border-radius: 3px; background: #fff; border: 2px solid #2a7fd4; cursor: ew-resize; }"
        " .nxcut_labels { display: flex; justify-content: space-between; font-size: 85%; opacity: .75; }")

    def part_cut_slider(self, p: base.Page):
        """ Leiste unter dem Video: Schieberegler fuer Start und Ende, markierter Abschnitt und Abspielposition """
        p('<style>' + self.SLIDER_CSS + '</style>')  # im Inhalt, p.style() landet im Kopf, den /pysys/ui nicht uebernimmt
        p('<div class="nxcut"><div class="nxcut_track"></div><div class="nxcut_sel" id="nxcut_sel"></div>'
          '<div class="nxcut_head" id="nxcut_head"></div>'
          '<input type="range" id="nxcut_start" min="0" max="0" step="0.01" value="0"'
          ' title="Start des Abschnitts (ziehen, das Video springt mit)">'
          '<input type="range" id="nxcut_end" min="0" max="0" step="0.01" value="0"'
          ' title="Ende des Abschnitts (ziehen, das Video springt mit)"></div>')
        p('<div class="nxcut_labels"><span id="nxcut_lstart" title="Start">0:00.0</span>'
          '<span id="nxcut_lpos" title="Aktuelle Position">0:00.0</span>'
          '<span id="nxcut_lend" title="Ende">0:00.0</span></div>')

    def part_slider_js(self, p: base.Page):
        p.script("""
            function nxcut_fmt(t) {
                t = Math.max(0, t || 0); var m = Math.floor(t / 60), s = (t - m * 60).toFixed(1);
                return m + ':' + (s < 10 ? '0' : '') + s;
            }
            function nxcut_pct(t) { var d = nxvideo_el('nxvideo').duration || 0; return d > 0 ? t / d * 100 : 0; }
            function nxcut_sync() {
                var d = nxvideo_el('nxvideo').duration || 0, s = nxvideo_time('nxvideo_start') || 0;
                var e = nxvideo_time('nxvideo_end'); if (e === null) e = d;
                ['nxcut_start', 'nxcut_end'].forEach(function(id) { nxvideo_el(id).max = d.toFixed(2); });
                nxvideo_el('nxcut_start').value = s; nxvideo_el('nxcut_end').value = e;
                nxvideo_el('nxcut_sel').style.left = nxcut_pct(s) + '%';
                nxvideo_el('nxcut_sel').style.width = Math.max(0, nxcut_pct(e) - nxcut_pct(s)) + '%';
                nxvideo_el('nxcut_lstart').innerText = 'Start ' + nxcut_fmt(s);
                nxvideo_el('nxcut_lend').innerText = 'Ende ' + nxcut_fmt(e);
            }
            function nxcut_head() {
                var t = nxvideo_el('nxvideo').currentTime;
                nxvideo_el('nxcut_head').style.left = nxcut_pct(t) + '%';
                nxvideo_el('nxcut_lpos').innerText = nxcut_fmt(t);
            }
            function nxcut_drag(n) {
                var s = parseFloat(nxvideo_el('nxcut_start').value), e = parseFloat(nxvideo_el('nxcut_end').value);
                if (n === 'start' && s > e) { s = e; nxvideo_el('nxcut_start').value = s; }
                if (n === 'end' && e < s) { e = s; nxvideo_el('nxcut_end').value = e; }
                var t = n === 'start' ? s : e;
                nxvideo_el('nxvideo_' + n).value = t.toFixed(2);
                var v = nxvideo_el('nxvideo'); v.pause(); v.currentTime = t;
                nxvideo_info();
            }
            nxvideo_ready(function() {
                var v = nxvideo_el('nxvideo');
                v.addEventListener('loadedmetadata', function() { nxcut_sync(); nxcut_head(); });
                v.addEventListener('timeupdate', nxcut_head);
                nxvideo_el('nxcut_start').addEventListener('input', function() { nxcut_drag('start'); });
                nxvideo_el('nxcut_end').addEventListener('input', function() { nxcut_drag('end'); });
                if (v.readyState >= 1) { nxcut_sync(); nxcut_head(); }
            });
        """)

    def part_cut_form(self, p: base.Page, path: str):
        action = '/pysys/dispatcher'
        with p.t('form', method='get', action=action, id='nxvideo_form'):
            p(w.hidden('type', self.type) + w.hidden(self.type, 'download') + w.hidden('file', w.s(path)))
            with p.t('div', style='margin: 8px 0;'):
                p.input('start', hint="Startzeit in Sekunden oder mm:ss", id='nxvideo_start', value='0', size='8')
                p(w.button_js("Start = Position", 'nxvideo_set("start");',
                              hint="Aktuelle Wiedergabeposition als Startzeit übernehmen"))
                p.input('end', hint="Endzeit in Sekunden oder mm:ss, leer = bis zum Ende", id='nxvideo_end', size='8')
                p(w.button_js("Ende = Position", 'nxvideo_set("end");',
                              hint="Aktuelle Wiedergabeposition als Endzeit übernehmen"))
                p(w.button_js("Vorschau", 'nxvideo_preview();', hint="Ausgewählten Abschnitt abspielen"))
            p.form_select('quality', "Qualität", QUALITY_TITLES, value='medium', id='nxvideo_quality',
                          hint="Maximale Auflösung, Audio-Bitrate und Kompression bei automatischer Bitrate (H.264, MP4)")
            p.form_select('bitrate', "Bitrate", BITRATES, value='', id='nxvideo_bitrate',
                          hint="Feste Video-Bitrate für eine planbare Dateigröße; "
                               "automatisch = konstante Qualität, Größe hängt vom Inhalt ab")
            p.span('', id='nxvideo_info')
            p.input('submit', hint="Abschnitt beschneiden, komprimieren und herunterladen (kann dauern)",
                    type='submit', value="Herunterladen")

    def part_cut_js(self, p: base.Page):
        p.script("""
            function nxvideo_el(n) { return document.getElementById(n); }
            function nxvideo_time(n) {
                var v = String(nxvideo_el(n).value || '').trim(); if (v === '') return null;
                return v.split(':').reduce(function(a, b) { return a * 60 + parseFloat(b.replace(',', '.')); }, 0);
            }
            function nxvideo_info() {
                var s = nxvideo_time('nxvideo_start') || 0, e = nxvideo_time('nxvideo_end');
                if (e === null) e = nxvideo_el('nxvideo').duration || 0;
                var len = Math.max(0, e - s), info = ' Länge: ' + len.toFixed(1) + ' s';
                var vb = parseInt(nxvideo_el('nxvideo_bitrate').value || '0');
                if (vb > 0) {  // Groesse = (Video- + Audio-Bitrate) * Laenge
                    var ab = {high: 160, medium: 128, low: 96}[nxvideo_el('nxvideo_quality').value] || 128;
                    info += ', ca. ' + ((vb + ab) * len / 8 / 1024).toFixed(1) + ' MB';
                }
                nxvideo_el('nxvideo_info').innerText = info + ' ';
                if (typeof nxcut_sync === 'function') nxcut_sync();
            }
            function nxvideo_set(n) {
                nxvideo_el('nxvideo_' + n).value = nxvideo_el('nxvideo').currentTime.toFixed(2); nxvideo_info();
            }
            function nxvideo_preview() {
                var v = nxvideo_el('nxvideo'); v.currentTime = nxvideo_time('nxvideo_start') || 0; v.play();
            }
            function nxvideo_ready(fn) {
                // in /pysys/ui wird die Seite erst nach dem Laden eingefuegt, DOMContentLoaded kommt dann nicht mehr
                if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', fn); else fn();
            }
            nxvideo_ready(function() {
                var v = nxvideo_el('nxvideo');
                if (v.readyState >= 1) nxvideo_info();
                v.addEventListener('loadedmetadata', nxvideo_info);
                v.addEventListener('timeupdate', function() {
                    var e = nxvideo_time('nxvideo_end'); if (e !== null && v.currentTime >= e) v.pause();
                });
                nxvideo_el('nxvideo_start').addEventListener('input', nxvideo_info);
                nxvideo_el('nxvideo_end').addEventListener('input', nxvideo_info);
                nxvideo_el('nxvideo_bitrate').addEventListener('change', nxvideo_info);
                nxvideo_el('nxvideo_quality').addEventListener('change', nxvideo_info);
            });
        """)


class Videos(r.BaseJobExecutor):
    """
    Zeigt die Videos eines Verzeichnisses, ein ausgewaehltes Video wird im Video-Runner bearbeitet.
    """

    def __init__(self, path: str = '~/Videos', video: str = 'video', type: str = 'videos'):
        super().__init__(type)
        self.path = os.path.realpath(os.path.expanduser(path))
        self.video_type = video

    # --- Video-Runner -------------------------------------------------------------------------

    def video_runner(self):
        """ Video-Runner aus dem Dispatcher, wird bei Bedarf angelegt; erlaubt das Verzeichnis dieses Runners """
        def find(d):
            runner = d.get_runner(self.video_type)
            if runner is None:
                d.add_runner(Video(type=self.video_type))
                runner = d.get_runner(self.video_type)
            if runner is not None and hasattr(runner, 'allow'):
                runner.allow(self.path)
            return runner
        return self.onParentClass(r.LazyDispatcher, find, None)

    def video_link(self, path: str = None) -> str:
        args = {'type': self.video_type, self.video_type: 'index'}
        if path is not None:
            args['file'] = path
        return '?' + urllib.parse.urlencode(args)

    # --- Dateien ------------------------------------------------------------------------------

    def resolve_dir(self, rel: str) -> str:
        """ :return: absolutes Verzeichnis innerhalb von path, sonst path """
        path = os.path.realpath(os.path.join(self.path, str(rel or '').lstrip('/\\')))
        if path == self.path or path.startswith(self.path.rstrip(os.sep) + os.sep):
            if os.path.isdir(path):
                return path
        return self.path

    def rel(self, path: str) -> str:
        return os.path.relpath(path, self.path).replace('\\', '/')

    def dir_videos(self, path: str) -> list:
        """ Videos direkt in path (nicht rekursiv, Unterordner werden erst ueber Browse gelesen) """
        try:
            with os.scandir(path) as it:
                return [e.path for e in it if e.is_file() and e.name.rsplit('.', 1)[-1].lower() in VIDEO_EXTS]
        except OSError:
            return []

    @op("Liste der Videos in dir (relativ zu path, nicht rekursiv)", result={'success': 'bool', 'videos': 'list'})
    def list_videos(self, dir: str = ''):
        return self.success('ok', videos=[self.rel(f) for f in self.dir_videos(self.resolve_dir(dir))])

    # --- Seiten -------------------------------------------------------------------------------

    def nav(self, p: base.Page, active=None):
        self.video_runner()
        p.nav(("Index", self.link(self.part_index), "Videos im Hauptverzeichnis"),
              ("Browse", self.link(self.part_browse), "Verzeichnisse durchsuchen"),
              ("Videos", self.video_link(), "Video-Editor mit Upload"),
              active=active)

    FEED_CSS = (".nxfeed { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 14px; }"
                " .nxfeed a { color: inherit; text-decoration: none; display: block; border-radius: 8px; overflow: hidden;"
                " background: rgba(128,128,128,.10); }"
                " .nxfeed a:hover { background: rgba(128,128,128,.22); }"
                " .nxfeed video { width: 100%; aspect-ratio: 16 / 9; object-fit: cover; background: #000; display: block;"
                " pointer-events: none; }"
                " .nxfeed .nxfeed_t { padding: 6px 9px; font-weight: 600; overflow: hidden; text-overflow: ellipsis;"
                " white-space: nowrap; }"
                " .nxfeed .nxfeed_m { padding: 0 9px 8px; font-size: 85%; opacity: .65; }")

    PAGE_SIZE = 24

    def stream_link(self, path: str) -> str:
        args = {'type': self.video_type, self.video_type: 'stream', 'file': path}
        return '/pysys/dispatcher?' + urllib.parse.urlencode(args) + '#t=1'  # absolut, auch in /pysys/ui

    def feed_item(self, path: str, title: str = None) -> str:
        """ Kachel mit Vorschau (spielt stumm beim Darueberfahren), Klick oeffnet den Video-Editor """
        title = title or self.rel(path)
        meta = format_size(os.path.getsize(path)) + ' · ' + time.strftime('%d.%m.%Y %H:%M',
                                                                       time.localtime(os.path.getmtime(path)))
        video = f'<video src="{w.s(self.stream_link(path))}" muted loop playsinline preload="metadata"></video>'
        hint = w.s("Im Video-Editor öffnen: " + path)
        return (f'<a href="{w.s(self.video_link(path))}" title="{hint}"'
                ' onmouseenter="this.querySelector(\'video\').play()"'
                ' onmouseleave="this.querySelector(\'video\').pause()">'
                f'{video}<div class="nxfeed_t">{w.s(title)}</div><div class="nxfeed_m">{meta}</div></a>')

    def part_feed(self, p: base.Page, videos: list, params={}, titles=None):
        """ Feed der Videos, neueste zuerst, seitenweise (Parameter pg) """
        videos = sorted(videos, key=os.path.getmtime, reverse=True)
        page = max(0, int(params.get('pg', 0) or 0))
        items = videos[page * self.PAGE_SIZE:(page + 1) * self.PAGE_SIZE]
        p('<style>' + self.FEED_CSS + '</style>')
        if not items:
            p.div("Keine Videos gefunden", style='opacity: .65;')
        p('<div class="nxfeed">' + ''.join(self.feed_item(f, titles(f) if titles else None) for f in items) + '</div>')
        self.part_pager(p, page, len(videos), params)

    def part_pager(self, p: base.Page, page: int, total: int, params={}):
        pages = (total + self.PAGE_SIZE - 1) // self.PAGE_SIZE
        if pages <= 1:
            return
        current = params.get(self.type, 'index')
        extra = {k: params[k] for k in ['dir'] if params.get(k)}
        links = []
        if page > 0:
            links.append(w.a("« Neuer", self.link(current, pg=page - 1, **extra), hint="Neuere Videos"))
        links.append(f'Seite {page + 1} / {pages}')
        if page + 1 < pages:
            links.append(w.a("Älter »", self.link(current, pg=page + 1, **extra), hint="Ältere Videos"))
        p.div(' &nbsp; '.join(links), style='margin: 12px 0; text-align: center;')

    def part_index(self, p: base.Page, params={}):
        self.nav(p, "Index")
        if not os.path.isdir(self.path):
            p.div("Verzeichnis nicht gefunden: " + w.s(self.path), style='color: #c33;')
            return
        self.part_feed(p, self.dir_videos(self.path), params)

    def part_browse(self, p: base.Page, params={}):
        self.nav(p, "Browse")
        current = self.resolve_dir(params.get('dir', ''))
        rel = '' if current == self.path else self.rel(current)
        p.h2(w.s('/' + rel))
        if rel != '':
            p.div(w.a(".. (übergeordnetes Verzeichnis)", self.link(self.part_browse, dir=os.path.dirname(rel)),
                      hint="Übergeordnetes Verzeichnis öffnen"))
        p.grid(self.browse_rows(current), [
            w.Grid.col('thumb', "", value=self.browse_thumb, html=True, sortable=False, width='130px'),
            w.Grid.col('name', "Name", value=self.browse_name, html=True, sort=lambda r: r['name'].lower()),
            w.Grid.col('kind', "Typ"),
            w.Grid.col('size', "Größe", value=lambda r: format_size(r['size']) if r['kind'] == 'Video' else '',
                       sort=lambda r: r['size']),
            w.Grid.col('mtime', "Geändert", value=lambda r: time.strftime('%d.%m.%Y %H:%M', time.localtime(r['mtime'])),
                       sort=lambda r: r['mtime'])],
            params=params, page_size=50, search=lambda r: r['name'], empty="Keine Videos oder Verzeichnisse",
            count="{shown} / {total} Einträge")

    def browse_rows(self, current: str) -> list:
        """ Unterverzeichnisse und Videos direkt in current (Verzeichnisse zuerst) """
        rows = []
        with os.scandir(current) as it:
            for e in it:
                if e.name.startswith('.'):
                    continue
                if e.is_dir():
                    kind = 'Verzeichnis'
                elif e.is_file() and e.name.rsplit('.', 1)[-1].lower() in VIDEO_EXTS:
                    kind = 'Video'
                else:
                    continue
                st = e.stat()
                rows.append({'name': e.name, 'kind': kind, 'path': e.path, 'size': st.st_size, 'mtime': st.st_mtime})
        return sorted(rows, key=lambda r: (r['kind'] != 'Verzeichnis', r['name'].lower()))

    def thumb_link(self, path: str) -> str:
        args = {'type': self.video_type, self.video_type: 'thumb', 'file': path}
        return '/pysys/dispatcher?' + urllib.parse.urlencode(args)  # absolut, auch in /pysys/ui

    def browse_thumb(self, row: dict) -> str:
        """ Vorschaubild (wird erst beim Sichtbarwerden geladen), Klick oeffnet den Video-Editor """
        if row['kind'] != 'Video':
            return ''
        img = (f'<img src="{w.s(self.thumb_link(row["path"]))}" loading="lazy" alt="" '
               'style="width: 120px; aspect-ratio: 16 / 9; object-fit: cover; background: #000; display: block;'
               ' border-radius: 4px;">')
        return w.a(img, self.video_link(row['path']), hint="Im Video-Editor öffnen: " + row['path'])

    def browse_name(self, row: dict) -> str:
        if row['kind'] == 'Verzeichnis':
            return w.a(w.s(row['name'] + '/'), self.link(self.part_browse, dir=self.rel(row['path'])),
                       hint="Verzeichnis öffnen")
        return w.a(w.s(row['name']), self.video_link(row['path']), hint="Im Video-Editor öffnen: " + row['path'])
