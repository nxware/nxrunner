import array
import collections
import html
import math
import os
import re
import subprocess
import sys
import tempfile
import time

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d
from nwebclient.runner import op


class Convert(r.BaseJobExecutor):
    """
      Konvertiert Audiodateien von WAV nach MP3 (pydub, benoetigt ffmpeg).
      op=tomp3 infile=/pfad/in.wav outfile=/pfad/out.mp3
    """

    MODULES = ['pydub']

    def __init__(self):
        super().__init__('audio_convert')
        self.define_sig(d.PStr('op', 'tomp3'), d.Param('infile', 'str'),
                        d.Param('outfile', 'str'))

    def to_mp3(self, infile, outfile):
        self.info(f"to_mp3({infile}, {outfile})")
        import pydub
        sound = pydub.AudioSegment.from_wav(infile)
        sound.export(outfile, format="mp3")

    def execute_tomp3(self, data):
        self.to_mp3(data['infile'], data['outfile'])
        return self.success()


# --- Morse-Code: Text <-> Zeichen ---------------------------------------------------------------

MORSE = {
    'A': '.-', 'B': '-...', 'C': '-.-.', 'D': '-..', 'E': '.', 'F': '..-.', 'G': '--.', 'H': '....', 'I': '..',
    'J': '.---', 'K': '-.-', 'L': '.-..', 'M': '--', 'N': '-.', 'O': '---', 'P': '.--.', 'Q': '--.-', 'R': '.-.',
    'S': '...', 'T': '-', 'U': '..-', 'V': '...-', 'W': '.--', 'X': '-..-', 'Y': '-.--', 'Z': '--..',
    '0': '-----', '1': '.----', '2': '..---', '3': '...--', '4': '....-', '5': '.....', '6': '-....',
    '7': '--...', '8': '---..', '9': '----.',
    '.': '.-.-.-', ',': '--..--', '?': '..--..', "'": '.----.', '!': '-.-.--', '/': '-..-.', '(': '-.--.',
    ')': '-.--.-', '&': '.-...', ':': '---...', ';': '-.-.-.', '=': '-...-', '+': '.-.-.', '-': '-....-',
    '_': '..--.-', '"': '.-..-.', '$': '...-..-', '@': '.--.-.',
    'Ä': '.-.-', 'Ö': '---.', 'Ü': '..--', 'ß': '...--..',
}
MORSE_REVERSE = {v: k for k, v in MORSE.items()}


def text_to_morse(text: str) -> str:
    """ 'SOS HI' -> '... --- ... / .... ..', unbekannte Zeichen werden ausgelassen """
    words = []
    for word in str(text).upper().split():
        codes = [MORSE[c] for c in word if c in MORSE]
        if codes:
            words.append(' '.join(codes))
    return ' / '.join(words)


def morse_to_text(code: str) -> str:
    """ '... --- ... / .... ..' -> 'SOS HI', unbekannte Codes werden zu '?' (Worttrenner: / oder 3 Leerzeichen) """
    code = str(code).replace('_', '-').replace('·', '.').replace('   ', ' / ')
    words = []
    for word in code.split('/'):
        chars = [MORSE_REVERSE.get(c, '?') for c in word.split()]
        if chars:
            words.append(''.join(chars))
    return ' '.join(words)


def morse_timings(code: str) -> list:
    """ :return: [(an: bool, Einheiten: int)] nach ITU (Punkt 1, Strich 3, Zeichen-Luecke 3, Wort-Luecke 7) """
    res = []
    for wi, word in enumerate(w for w in code.split('/') if w.strip()):
        if wi > 0:
            res.append((False, 7))
        for ci, char in enumerate(word.split()):
            if ci > 0:
                res.append((False, 3))
            for si, sym in enumerate(char):
                if si > 0:
                    res.append((False, 1))
                res.append((True, 3 if sym == '-' else 1))
    return res


# --- Morse-Code: Audio erzeugen -----------------------------------------------------------------

def tone_pcm(samples: int, freq: float, rate: int, amplitude: float = 0.6):
    """ Sinuston als array('h') mit 5ms Rampen gegen Klicks """
    ramp = max(1, min(int(rate * 0.005), samples // 2))
    peak = 32767 * amplitude
    data = array.array('h', bytes(samples * 2))
    for i in range(samples):
        env = min(1.0, i / ramp, (samples - 1 - i) / ramp)
        data[i] = int(peak * env * math.sin(2 * math.pi * freq * i / rate))
    return data


def synth_pcm(code: str, wpm: float = 20, freq: float = 600, rate: int = 22050, padding: float = 0.5):
    """ Morse-Code als 16bit Mono PCM (array('h')), wpm nach PARIS-Norm: Einheit = 1.2 / wpm Sekunden """
    unit = int(rate * 1.2 / max(1.0, float(wpm)))
    tones = {1: tone_pcm(unit, freq, rate), 3: tone_pcm(unit * 3, freq, rate)}
    pad = array.array('h', bytes(int(rate * padding) * 2))
    data = array.array('h', pad)
    for on, units in morse_timings(code):
        data.extend(tones[units] if on else array.array('h', bytes(unit * units * 2)))
    data.extend(pad)
    return data


# --- Morse-Code: Audio decodieren ---------------------------------------------------------------

def envelope(samples, rate: int, window_ms: float = 5) -> list:
    """ mittlere Amplitude je Fenster """
    n = max(1, int(rate * window_ms / 1000))
    return [sum(map(abs, samples[i:i + n])) / n for i in range(0, len(samples) - n + 1, n)]


def to_runs(env: list, min_len: int = 2) -> list:
    """ Schwellwert (Mitte zwischen Rauschen und Ton) -> [[an, Fenster]], Stoerungen < min_len werden verschmolzen """
    if not env:
        return []
    s = sorted(env)
    lo, hi = s[len(s) // 10], s[int(len(s) * 0.98)]
    if hi <= 0 or hi < lo * 2:
        return []
    thr = lo + (hi - lo) * 0.5
    runs = []
    for v in env:
        on = v > thr
        if runs and runs[-1][0] == on:
            runs[-1][1] += 1
        else:
            runs.append([on, 1])
    return merge_short_runs(runs, min_len)


def merge_short_runs(runs: list, min_len: int) -> list:
    """ zu kurze Abschnitte dem vorherigen zuschlagen, Stille am Anfang und Ende entfernen """
    res = []
    for on, n in runs:
        if res and (n < min_len or res[-1][0] == on):
            res[-1][1] += n
        else:
            res.append([on, n])
    while res and not res[0][0]:
        res.pop(0)
    while res and not res[-1][0]:
        res.pop()
    return res


def estimate_unit(runs: list) -> float:
    """
      Laenge einer Einheit (Punkt) in Fenstern: Median der kuerzesten Gruppe aller Ton- und Pausenlaengen
      (Punkte und Luecken innerhalb eines Zeichens sind 1 Einheit lang)
    """
    lengths = sorted(n for _, n in runs)
    if not lengths:
        return 1.0
    group = [lengths[0]]
    for n in lengths[1:]:
        if n > group[-1] * 1.8:
            break
        group.append(n)
    return float(group[len(group) // 2])


def runs_to_morse(runs: list, unit: float) -> str:
    """ Ton >= 2 Einheiten: Strich, Pause >= 2: Zeichenende, >= 5: Wortende """
    out = ''
    for on, n in runs:
        units = n / unit
        if on:
            out += '-' if units >= 2 else '.'
        elif units >= 5:
            out += ' / '
        elif units >= 2:
            out += ' '
    return out


def decode_pcm(samples, rate: int, window_ms: float = 5) -> dict:
    """ :return: {morse, text, wpm} aus PCM-Samples """
    runs = to_runs(envelope(samples, rate, window_ms))
    if not runs:
        return {'morse': '', 'text': '', 'wpm': 0}
    unit = estimate_unit(runs)
    morse = runs_to_morse(runs, unit)
    units = sum(units for _, units in morse_timings(morse))
    if units > 0:  # genauer: Gesamtlaenge / Anzahl Einheiten
        unit = sum(n for _, n in runs) / units
    return {'morse': morse, 'text': morse_to_text(morse), 'wpm': round(1200 / (unit * window_ms), 1)}


class MorseStreamDecoder:
    """
      Decodiert Morse-Code fortlaufend aus PCM-Bloecken (feed), z.B. von einer Soundkarte.

      Pegel je Fenster (window_ms): mittlere Amplitude oder, mit freq > 0, Goertzel-Filter auf die Tonhoehe.
      Schwelle adaptiv aus den letzten history_s Sekunden (Mitte zwischen Rauschen und Ton), squelch ist der
      Mindestpegel eines Tons. Die Punktlaenge wird aus den letzten Ton-/Pausenlaengen nachgefuehrt.
      Callbacks: on_char(char, code), on_word(word)
    """

    MIN_RUNS = 8  # ab so vielen Abschnitten wird die Punktlaenge geschaetzt, vorher gilt wpm
    WAIT_MS = 1500  # vor MIN_RUNS: Ausgabe nach so langer Pause
    MIN_HISTORY_MS = 200  # Schwelle erst ab so viel gemessenem Pegel

    def __init__(self, rate: int, freq: float = 0, wpm: float = 20, squelch: float = 100, window_ms: float = 5,
                 history_s: float = 4, on_char=None, on_word=None):
        self.n = max(1, int(rate * window_ms / 1000))
        self.window_ms = window_ms
        self.coeff = 2 * math.cos(2 * math.pi * float(freq) / rate) if freq and float(freq) > 0 else None
        self.squelch = float(squelch)
        self.on_char, self.on_word = on_char, on_word
        self.rest = array.array('h')
        self.env = collections.deque(maxlen=max(20, int(history_s * 1000 / window_ms)))
        self.runs = collections.deque(maxlen=60)
        self.recent = collections.deque(maxlen=60)  # letzte Pegel, fuer den Beginn eines Signals
        self.unit = 1200 / max(1.0, float(wpm)) / window_ms
        self.speed_unit = None  # genauere Punktlaenge aus Gesamtlaenge / Einheiten (nur fuer die Anzeige)
        self.thr, self.level, self.count = None, 0.0, 0
        self.state, self.length, self.pending = False, 0, 0
        self.open_runs, self.word = [], ''
        self.word_done = True

    @property
    def wpm(self) -> float:
        return round(1200 / ((self.speed_unit or self.unit) * self.window_ms), 1)

    def feed(self, samples):
        """ PCM-Samples (array('h') oder Liste von int) verarbeiten """
        data = self.rest + array.array('h', samples)
        end = len(data) - len(data) % self.n
        for i in range(0, end, self.n):
            self.step(self.measure(data[i:i + self.n]))
        self.rest = data[end:]

    def measure(self, chunk) -> float:
        if self.coeff is None:
            return sum(map(abs, chunk)) / len(chunk)
        s1 = s2 = 0.0
        c = self.coeff
        for x in chunk:
            s1, s2 = x + c * s1 - s2, s1
        return math.sqrt(max(0.0, s1 * s1 + s2 * s2 - c * s1 * s2)) * 2 / len(chunk)

    def update_threshold(self):
        """
          Schwelle zwischen Rauschen (lo) und Ton (hi); None ohne Signal. Goertzel-Pegel von Rauschen streuen
          stark, dort wird daher mehr Abstand verlangt
        """
        if len(self.env) * self.window_ms < self.MIN_HISTORY_MS:
            return
        s = sorted(self.env)
        if self.coeff is None:
            lo, ratio = s[len(s) // 10], 2
        else:
            lo, ratio = s[len(s) // 4], 6
        hi = s[int(len(s) * 0.98)]
        self.thr = lo + (hi - lo) * 0.5 if hi >= max(self.squelch, lo * ratio) else None

    def step(self, level: float):
        """ ein Fenster: Schwelle nachfuehren, bei erstem Signal die letzten Fenster erneut auswerten """
        self.level = level
        self.env.append(level)
        self.count += 1
        no_signal = self.thr is None
        if self.count % 20 == 0 or no_signal and level > self.squelch:
            self.update_threshold()
        if no_signal and self.thr is not None:
            for v in self.recent:
                self.track(v)
        self.recent.append(level)
        self.track(level)

    def track(self, level: float):
        """ Ton/Pause mit Entprellung ueber 2 Fenster, Luecken pruefen """
        on = self.thr is not None and level > self.thr
        if on == self.state:
            self.length += self.pending + 1
            self.pending = 0
        else:
            self.pending += 1
            if self.pending >= 2:
                self.finish_run(self.state, self.length)
                self.state, self.length, self.pending = on, self.pending, 0
        if not self.state:
            self.check_gap()

    def finish_run(self, on: bool, n: int):
        """ abgeschlossener Ton/Pause: fuer die Punktlaenge merken, Toene und Pausen dazwischen bis zur Ausgabe """
        if n <= 0:
            return
        self.runs.append([on, n])
        if on or self.open_runs:
            self.open_runs.append([on, n])
        if len(self.runs) >= self.MIN_RUNS:
            self.unit = self.clamp_unit(estimate_unit(list(self.runs)))

    def clamp_unit(self, unit: float) -> float:
        """ Begrenzung auf 5..60 WPM """
        return min(240 / self.window_ms, max(20 / self.window_ms, unit))

    def check_gap(self):
        """
          in einer Pause: offene Toene ausgeben, sobald die Pause ein Zeichenende ist; vor MIN_RUNS
          erst nach einer langen Pause (dann Punktlaenge aus dem bisher Gehoerten)
        """
        if self.open_runs:
            ready = len(self.runs) >= self.MIN_RUNS
            if not ready and self.length * self.window_ms >= self.WAIT_MS:
                self.unit = self.clamp_unit(estimate_unit(list(self.runs)))
                ready = True
            if ready and self.length >= 2 * self.unit:
                self.decode_open()
        if not self.word_done and self.length >= 5 * self.unit:
            self.emit_word()

    def decode_open(self):
        """ offene Toene/Pausen mit der aktuellen Punktlaenge in Zeichen und Woerter umsetzen """
        morse = runs_to_morse(self.open_runs, self.unit)
        units = sum(u for _, u in morse_timings(morse))
        if units > 0:
            self.speed_unit = sum(n for _, n in self.open_runs) / units
        self.open_runs = []
        for i, word in enumerate(morse.split('/')):
            if i > 0:
                self.emit_word()
            for code in word.split():
                self.emit_char(code)

    def emit_char(self, code: str):
        char = MORSE_REVERSE.get(code, '?')
        self.word += char
        self.word_done = False
        if self.on_char:
            self.on_char(char, code)

    def emit_word(self):
        self.word_done = True
        word, self.word = self.word, ''
        if word and self.on_word:
            self.on_word(word)

    def flush(self):
        """ offene Zeichen und Wort ausgeben (z.B. beim Stoppen) """
        if self.state:
            self.finish_run(True, self.length)
            self.state, self.length = False, 0
        if self.open_runs:
            if len(self.runs) < self.MIN_RUNS:
                self.unit = self.clamp_unit(estimate_unit(list(self.runs)))
            self.decode_open()
        self.emit_word()


# --- Runner -------------------------------------------------------------------------------------

class Morse(r.BaseJobExecutor):
    """
      Morse-Code: erzeugt eine MP3 (oder WAV/OGG, je nach Endung) aus Text und decodiert Morse-Code aus
      einer Audiodatei. Benoetigt ffmpeg (im PATH oder per Parameter ffmpeg).

      Config (runners):
          morse: nxrunner.audio:Morse
          morse: nxrunner.audio:Morse(path="~/morse", ffmpeg="/usr/bin/ffmpeg")

      Jobs:
          {"type": "morse", "op": "encode", "text": "SOS", "wpm": 20, "freq": 600}
          {"type": "morse", "op": "decode", "file": "/pfad/morse.mp3"}
          {"type": "morse", "op": "text2morse", "text": "SOS"}
    """

    RATE = 22050
    DECODE_RATE = 8000
    AUDIO_EXTS = {'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'ogg': 'audio/ogg'}
    MAX_TEXT = 2000

    def __init__(self, type='morse', path=None, ffmpeg='ffmpeg'):
        """
        :param path: Verzeichnis fuer erzeugte und hochgeladene Dateien (Standard: Temp-Verzeichnis)
        :param ffmpeg: Pfad zu ffmpeg
        """
        super().__init__(type)
        self.path = os.path.expanduser(path) if path else os.path.join(tempfile.gettempdir(), 'nxrunner_morse')
        self.ffmpeg = ffmpeg
        os.makedirs(self.path, exist_ok=True)

    # --- ffmpeg -------------------------------------------------------------------------------

    def run_ffmpeg(self, args: list, data: bytes = None) -> bytes:
        try:
            res = subprocess.run([self.ffmpeg, '-hide_banner', '-loglevel', 'error', '-y'] + args, input=data,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except FileNotFoundError:
            raise RuntimeError(f'ffmpeg nicht gefunden ({self.ffmpeg})')
        if res.returncode != 0:
            raise RuntimeError('ffmpeg: ' + res.stderr.decode('utf-8', errors='replace').strip())
        return res.stdout

    def write_audio(self, pcm, outfile: str):
        """ PCM (array('h'), RATE) per ffmpeg als Datei speichern, Format nach Endung """
        if sys.byteorder == 'big':
            pcm.byteswap()
        self.run_ffmpeg(['-f', 's16le', '-ar', str(self.RATE), '-ac', '1', '-i', 'pipe:0', outfile], pcm.tobytes())

    def read_audio(self, infile: str, freq: float = 0):
        """ :return: array('h') Mono mit DECODE_RATE, optional Bandpass um freq """
        af = ['-af', f'bandpass=f={float(freq)}:width_type=h:w=300'] if freq and float(freq) > 0 else []
        raw = self.run_ffmpeg(['-i', infile] + af + ['-f', 's16le', '-ac', '1', '-ar', str(self.DECODE_RATE),
                                                     'pipe:1'])
        pcm = array.array('h', raw[:len(raw) // 2 * 2])
        if sys.byteorder == 'big':
            pcm.byteswap()
        return pcm

    # --- Dateien ------------------------------------------------------------------------------

    def resolve(self, file: str):
        """ Datei: absolut oder relativ zu path; None wenn nicht vorhanden """
        if not file:
            return None
        path = os.path.expanduser(str(file))
        if not os.path.isabs(path):
            path = os.path.join(self.path, path)
        return path if os.path.isfile(path) else None

    def resolve_own(self, name: str):
        """ nur Audiodateien direkt in path (fuer Auslieferung per page_audio) """
        name = os.path.basename(str(name or ''))
        path = os.path.join(self.path, name)
        if name.rsplit('.', 1)[-1].lower() in self.AUDIO_EXTS and os.path.isfile(path):
            return path
        return None

    def own_file(self, path: str) -> bool:
        return os.path.dirname(os.path.realpath(path)) == os.path.realpath(self.path)

    def new_filename(self, text: str, ext: str = 'mp3', prefix: str = 'morse') -> str:
        slug = re.sub(r'[^A-Za-z0-9]+', '_', str(text)).strip('_')[:30] or 'audio'
        return f'{prefix}_{slug}_{time.strftime("%Y%m%d_%H%M%S")}.{ext}'

    def target_file(self, text: str, file: str = '') -> str:
        if not file:
            return os.path.join(self.path, self.new_filename(text))
        file = os.path.expanduser(file)
        return file if os.path.isabs(file) else os.path.join(self.path, file)

    def audio_url(self, path: str) -> str:
        return self.dispatcher_link(self.page_audio, file=os.path.basename(path))

    # --- Operationen --------------------------------------------------------------------------

    @op("Erzeugt eine Audiodatei (mp3/wav/ogg) mit dem Text als Morse-Code, wpm: Woerter pro Minute, "
        "freq: Tonhoehe in Hz, file: Zieldatei (leer: automatisch in path)",
        result={'success': 'bool', 'message': 'str', 'file': 'str', 'morse': 'str', 'url': 'str'})
    def encode(self, text: str = 'SOS', wpm: float = 20, freq: float = 600, file: str = ''):
        morse = text_to_morse(str(text)[:self.MAX_TEXT])
        if not morse:
            return self.fail('Kein morsbarer Text')
        if not (5 <= wpm <= 60) or not (100 <= freq <= 4000):
            return self.fail('wpm muss zwischen 5 und 60, freq zwischen 100 und 4000 Hz liegen')
        outfile = self.target_file(text, file)
        try:
            self.write_audio(synth_pcm(morse, wpm, freq, self.RATE), outfile)
        except (OSError, RuntimeError) as e:
            return self.fail(f'Fehler beim Erzeugen: {e}')
        self.info(f'encode: {morse} -> {outfile}')
        url = self.audio_url(outfile) if self.own_file(outfile) else ''
        return self.success(os.path.basename(outfile) + ' erzeugt', file=outfile, morse=morse, url=url)

    @op("Decodiert Morse-Code aus einer Audiodatei (absolut oder relativ zu path), "
        "freq: optional Tonhoehe in Hz fuer einen Bandpass-Filter (0: aus)",
        result={'success': 'bool', 'message': 'str', 'morse': 'str', 'text': 'str', 'wpm': 'float'})
    def decode(self, file: str = '', freq: float = 0):
        path = self.resolve(file)
        if path is None:
            return self.fail('Datei nicht gefunden: ' + str(file))
        try:
            res = decode_pcm(self.read_audio(path, freq), self.DECODE_RATE)
        except (OSError, RuntimeError, ValueError) as e:
            return self.fail(f'Fehler beim Lesen: {e}')
        if not res['morse']:
            return self.fail('Kein Morse-Signal erkannt', morse='', text='', wpm=0)
        self.info(f'decode: {path} -> {res["text"]}')
        return self.success(res['text'], **res)

    @op("Wandelt Text in Morse-Zeichen um (ohne Audio)", result={'success': 'bool', 'message': 'str', 'morse': 'str'})
    def text2morse(self, text: str = ''):
        morse = text_to_morse(text)
        return self.success(morse, morse=morse)

    @op("Wandelt Morse-Zeichen (. - Leerzeichen /) in Text um",
        result={'success': 'bool', 'message': 'str', 'text': 'str'})
    def morse2text(self, morse: str = ''):
        text = morse_to_text(morse)
        return self.success(text, text=text)

    @op("Audiodateien in path (neueste zuerst)", result={'success': 'bool', 'message': 'str', 'files': 'list'})
    def files(self):
        names = [n for n in os.listdir(self.path) if self.resolve_own(n)]
        names.sort(key=lambda n: os.path.getmtime(os.path.join(self.path, n)), reverse=True)
        return self.success(f'{len(names)} Dateien', files=names)

    @op("Loescht eine Audiodatei in path", result={'success': 'bool', 'message': 'str'})
    def delete(self, file: str = ''):
        path = self.resolve_own(file)
        if path is None:
            return self.fail('Datei nicht gefunden: ' + str(file))
        os.remove(path)
        return self.success(os.path.basename(path) + ' gelöscht', ui_reload=True)

    # --- Seiten -------------------------------------------------------------------------------

    def page_audio(self, params={}):
        """ liefert eine Audiodatei aus path (Player, Download mit download=1) """
        from flask import send_file
        path = self.resolve_own(params.get('file'))
        if path is None:
            return "404, Datei nicht gefunden", 404
        mime = self.AUDIO_EXTS[path.rsplit('.', 1)[-1].lower()]
        return send_file(path, mimetype=mime, as_attachment=bool(params.get('download')),
                         download_name=os.path.basename(path))

    def page_upload(self, params={}):
        """ POST (multipart): audio, freq; speichert die Datei in path und decodiert sie, Antwort JSON """
        from flask import jsonify
        dst = self.save_upload(w.uploaded_file(params, 'audio'))
        if dst is None:
            return jsonify(self.fail('Keine Audiodatei (erlaubt: ' + ', '.join(self.AUDIO_EXTS) + ')'))
        try:
            freq = float(params.get('freq') or 0)
        except ValueError:
            freq = 0
        res = self.decode(dst, freq)
        return jsonify({**res, 'file': os.path.basename(dst), 'url': self.audio_url(dst)})

    def save_upload(self, upload):
        """ hochgeladene Datei (dateiname, bytes) aus w.uploaded_file in path speichern, :return: Pfad oder None """
        if upload is None or not upload[1]:
            return None
        name = os.path.basename(upload[0] or '')
        ext = name.rsplit('.', 1)[-1].lower() if '.' in name else ''
        if ext not in self.AUDIO_EXTS:
            return None
        dst = os.path.join(self.path, self.new_filename(name.rsplit('.', 1)[0], ext, prefix='upload'))
        with open(dst, 'wb') as f:
            f.write(upload[1])
        return dst

    def part_index(self, p: b.Page, params={}):
        p.script(MORSE_JS)
        p('<style>' + MORSE_CSS + '</style>')
        self.part_encode_form(p)
        self.part_decode_form(p)
        self.part_convert_form(p)
        self.part_file_list(p)
        with p.section(h="Ergebnis"):
            p.pre('', id='result')

    def part_encode_form(self, p: b.Page):
        with p.section(h="Text → MP3"):
            p.form_input('morse_text', "Text", hint="Text, der gemorst werden soll", id='morse_text', value='SOS')
            p.form_input('morse_wpm', "Tempo (WPM)", hint="Wörter pro Minute (PARIS), 5 bis 60", id='morse_wpm',
                         type='number', value='20', min='5', max='60')
            p.form_input('morse_freq', "Ton (Hz)", hint="Tonhöhe in Hz, 100 bis 4000", id='morse_freq',
                         type='number', value='600', min='100', max='4000')
            p(self.action_btn_parametric("MP3 erzeugen", dict(type=self.type, op='encode', text='#morse_text',
                                                              wpm='#morse_wpm', freq='#morse_freq'),
                                         on_success='morse_encoded', hint="Erzeugt eine MP3 mit dem Morse-Code"))
            p.div('', id='morse_player', _class='morse_out')

    def part_decode_form(self, p: b.Page):
        with p.section(h="MP3 → Text"):
            p.form_input('morse_upload', "Audiodatei", hint="MP3, WAV oder OGG mit Morse-Code",
                         id='morse_upload', type='file', accept='.mp3,.wav,.ogg,audio/*')
            p.form_input('morse_dfreq', "Bandpass (Hz)", id='morse_dfreq', type='number', value='0', min='0',
                         hint="Tonhöhe des Morse-Signals für einen Bandpass-Filter gegen Störungen, 0 = aus")
            upload = json_str(self.dispatcher_link(self.page_upload))
            p(w.button_js("Hochladen & decodieren", f'morse_upload({upload});',
                          hint="Lädt die Datei hoch und decodiert den Morse-Code"))
            p.div('', id='morse_decoded', _class='morse_out')

    def part_convert_form(self, p: b.Page):
        with p.section(h="Text ↔ Morse-Zeichen"):
            p.form_input('morse_chars', "Text / Morse", id='morse_chars',
                         hint="Text oder Morse-Zeichen (. - Leerzeichen zwischen Zeichen, / zwischen Wörtern)")
            p(self.action_btn_parametric("→ Morse", dict(type=self.type, op='text2morse', text='#morse_chars'),
                                         on_success='morse_converted', hint="Text in Morse-Zeichen umwandeln"))
            p(self.action_btn_parametric("→ Text", dict(type=self.type, op='morse2text', morse='#morse_chars'),
                                         on_success='morse_converted', hint="Morse-Zeichen in Text umwandeln"))

    def part_file_list(self, p: b.Page):
        with p.section(h="Dateien"):
            p.div("Verzeichnis: " + w.s(self.path), _class='morse_hint')
            for name in self.files()['files'][:30]:
                self.part_file_row(p, name)

    def part_file_row(self, p: b.Page, name: str):
        url = self.dispatcher_link(self.page_audio, file=name)
        p('<div class="morse_file">')
        p.span(w.s(name), _class='morse_name')
        p(f'<audio controls preload="none" src="{w.s(url)}"></audio> ')
        p(w.a("Download", self.dispatcher_link(self.page_audio, file=name, download=1),
              hint="Datei herunterladen") + ' ')
        p(self.action_btn(dict(type=self.type, op='decode', file=name), label="Decodieren",
                          hint="Morse-Code aus dieser Datei lesen") + ' ')
        p(self.action_btn(dict(type=self.type, op='delete', file=name), label="Löschen",
                          hint="Datei löschen"))
        p('</div>')


class MorseSoundcard(r.BaseJobExecutor):
    """
      Morse-Code ueber eine (USB-)Soundkarte (sounddevice/PortAudio), primaer fuer Linux/Raspberry Pi:
        - listen: liest fortlaufend vom Eingang und decodiert (MorseStreamDecoder), Ausgabe per self.info
          und Events morse_char {char, code, wpm} und morse_word {word, wpm}
        - play: gibt Text als Morse-Ton auf dem Ausgang aus

      Geraete: Index, (Teil des) Namens ohne Gross-/Kleinschreibung (z.B. "USB"), leer = Standardgeraet,
      siehe op=devices. Abtastrate und Kanalzahl werden automatisch passend gewaehlt (ALSA-hw-Geraete
      koennen oft nur 44100/48000 Hz und Stereo). Ist das Geraet nicht verfuegbar (z.B. USB-Soundkarte beim
      Booten noch nicht da oder abgezogen), wird alle RETRY_S Sekunden neu versucht, bis stop.

      Raspberry Pi:
          sudo apt install libportaudio2 ffmpeg
          pip install sounddevice
          sudo usermod -aG audio $USER        # Benutzer des Dienstes braucht Zugriff auf die Soundkarte
          arecord -l / aplay -l               # Soundkarten anzeigen
          alsamixer -c 1                      # F4: Aufnahmepegel der USB-Karte einstellen (nicht uebersteuern)
      Bei schwachem Signal squelch verringern, bei Stoerungen freq (Tonhoehe) setzen.

      Config (runners):
          morse_sc: nxrunner.audio:MorseSoundcard
          morse_sc: nxrunner.audio:MorseSoundcard(device="USB", out_device="USB", freq=700, autostart=True)

      Jobs:
          {"type": "morse_soundcard", "op": "listen", "device": "USB", "freq": 700}
          {"type": "morse_soundcard", "op": "play", "text": "CQ CQ", "wpm": 20, "freq": 600}
    """

    MODULES = ['sounddevice']
    BLOCK_S = 0.05
    MAX_TEXT = 4000
    RATES = (48000, 44100, 16000, 8000)  # nach der Standardrate des Geraets probiert
    RETRY_S = 10

    def __init__(self, type='morse_soundcard', device='', out_device='', freq=0, wpm=20, squelch=100,
                 autostart=False):
        """
        :param device: Eingangsgeraet fuer listen
        :param out_device: Ausgangsgeraet fuer play
        :param freq: Tonhoehe fuer den Goertzel-Filter beim Empfang (0: nur Lautstaerke)
        :param wpm: angenommenes Tempo bis zur ersten Schaetzung
        :param squelch: Mindestpegel eines Tons (Amplitude 16bit)
        :param autostart: listen beim Start
        """
        super().__init__(type)
        self.device, self.out_device = device, out_device
        self.freq, self.wpm, self.squelch = float(freq), float(wpm), float(squelch)
        self.running = self.playing = False
        self.decoder = None
        self.text = ''
        self.error_msg = ''
        self.overflows = 0
        if str(autostart).lower() in ('1', 'true', 'yes'):
            self.listen(device, freq)

    # --- Geraete ------------------------------------------------------------------------------

    @staticmethod
    def resolve_device(sd, device, kind: str):
        """
          '' -> None (Standard), '3' -> 3, sonst erstes Geraet mit Kanaelen fuer kind ('input'/'output'),
          dessen Name device enthaelt (Gross-/Kleinschreibung egal)
        """
        device = '' if device is None else str(device).strip()
        if device == '':
            return None
        if device.isdigit():
            return int(device)
        key = 'max_input_channels' if kind == 'input' else 'max_output_channels'
        for dev in sd.query_devices():
            if dev[key] > 0 and device.lower() in dev['name'].lower():
                return dev['index']
        raise ValueError(f'Kein {kind}-Geraet mit "{device}" im Namen gefunden')

    def find_settings(self, sd, device, kind: str):
        """ :return: (rate, kanaele), die das Geraet unterstuetzt: mono bevorzugt, Standardrate zuerst """
        info = sd.query_devices(device, kind)
        max_ch = int(info['max_input_channels' if kind == 'input' else 'max_output_channels'])
        check = sd.check_input_settings if kind == 'input' else sd.check_output_settings
        default = int(info['default_samplerate'])
        rates = [default] + [x for x in self.RATES if x != default]
        error = None
        for channels in sorted({1, min(2, max_ch), max_ch}):
            for rate in rates:
                try:
                    check(device=device, channels=channels, dtype='int16', samplerate=rate)
                    return rate, channels
                except Exception as e:
                    error = e
        raise RuntimeError(f'Keine passende Einstellung fuer {kind} {device}: {error}')

    @op("Audiogeraete (in/out: Anzahl Kanaele)", result={'success': 'bool', 'message': 'str', 'devices': 'list'})
    def devices(self):
        import sounddevice as sd
        res = []
        for dev in sd.query_devices():
            api = sd.query_hostapis(dev['hostapi'])['name']
            res.append({'index': dev['index'], 'name': dev['name'], 'hostapi': api,
                        'in': dev['max_input_channels'], 'out': dev['max_output_channels'],
                        'samplerate': dev['default_samplerate']})
        return self.success(f'{len(res)} Geraete', devices=res)

    def device_options(self, kind: str, current) -> dict:
        try:
            devs = self.devices()['devices']
        except Exception as e:
            return {'': f'Standard (Fehler: {e})'}
        res = {'': 'Standard', **{str(d['index']): f"{d['index']}: {d['name']} ({d['hostapi']})"
                                  for d in devs if d[kind] > 0}}
        if current and str(current) not in res:
            res[str(current)] = f'"{current}" (Name)'  # konfiguriert per Name
        return res

    # --- Empfang ------------------------------------------------------------------------------

    @op("Startet den Empfang vom Eingang device, freq: Tonhoehe fuer den Filter (0: aus, -1: wie bisher)",
        result={'success': 'bool', 'message': 'str'})
    def listen(self, device: str = '', freq: float = -1):
        import threading
        if self.running:
            return self.fail('Empfang laeuft bereits')
        self.device = device or self.device
        if freq >= 0:
            self.freq = freq
        self.running, self.error_msg = True, ''
        threading.Thread(target=self.read_loop, name='morse_listen', daemon=True).start()
        return self.success('Empfang gestartet', ui_reload=True)

    @op("Stoppt den Empfang", result={'success': 'bool', 'message': 'str'})
    def stop(self):
        self.running = False
        return self.success('Empfang gestoppt', ui_reload=True)

    def read_loop(self):
        """ Empfangs-Thread: bei Fehlern (Geraet fehlt/abgezogen) alle RETRY_S Sekunden neu versuchen """
        while self.running:
            try:
                self.listen_once()
            except Exception as e:
                self.error_msg = f'{e} (neuer Versuch in {self.RETRY_S}s)'
                self.error(f'Morse-Empfang: {self.error_msg}')
                for _ in range(self.RETRY_S * 10):
                    if not self.running:
                        break
                    time.sleep(0.1)
        self.error_msg = ''
        self.info('Morse-Empfang beendet')

    def listen_once(self):
        """ Callback-Stream (geht mit allen PortAudio-Backends) -> Queue -> Decoder, bis stop """
        import queue
        import sounddevice as sd
        device = self.resolve_device(sd, self.device, 'input')
        rate, channels = self.find_settings(sd, device, 'input')
        self.decoder = MorseStreamDecoder(rate, self.freq, self.wpm, self.squelch,
                                          on_char=self.on_char, on_word=self.on_word)
        blocks = queue.Queue(maxsize=100)

        def callback(data, frames, t, status):
            try:
                blocks.put_nowait(bytes(data))
            except queue.Full:  # Verarbeitung zu langsam: Block verwerfen
                self.overflows += 1

        with sd.RawInputStream(samplerate=rate, device=device, channels=channels, dtype='int16',
                               blocksize=int(rate * self.BLOCK_S), callback=callback) as stream:
            self.error_msg = ''
            self.info(f'Morse-Empfang: {device if device is not None else "Standard"}, {rate} Hz, '
                      f'{channels} Kanal/Kanaele, Filter {self.freq or "aus"}')
            while self.running and stream.active:
                try:
                    self.decoder.feed(self.pcm(blocks.get(timeout=0.5), channels))
                except queue.Empty:
                    pass
            if self.running:
                raise RuntimeError('Audio-Stream beendet (Geraet entfernt?)')
        self.decoder.flush()

    @staticmethod
    def pcm(data: bytes, channels: int = 1):
        """ int16-Bytes -> array('h') des ersten Kanals """
        res = array.array('h', data)
        if sys.byteorder == 'big':
            res.byteswap()
        return res if channels <= 1 else res[::channels]

    def add_text(self, s: str):
        self.text = (self.text + s)[-self.MAX_TEXT:]

    def on_char(self, char: str, code: str):
        self.add_text(char)
        self.emit_event('morse_char', char=char, code=code, wpm=self.decoder.wpm)

    def on_word(self, word: str):
        self.add_text(' ')
        self.info(f'Morse: {word} ({self.decoder.wpm} WPM)')
        self.emit_event('morse_word', word=word, wpm=self.decoder.wpm)

    @op("Status und empfangener Text", result={'success': 'bool', 'message': 'str', 'running': 'bool',
                                                 'playing': 'bool', 'text': 'str', 'wpm': 'float',
                                                 'level': 'float', 'threshold': 'float', 'overflows': 'int'})
    def status(self):
        dec = self.decoder
        return self.success(self.error_msg or ('Empfang laeuft' if self.running else 'gestoppt'),
                            running=self.running, playing=self.playing, text=self.text,
                            wpm=dec.wpm if dec else 0, level=round(dec.level) if dec else 0,
                            threshold=round(dec.thr) if dec and dec.thr else 0, overflows=self.overflows)

    @op("Empfangenen Text leeren", result={'success': 'bool', 'message': 'str'})
    def clear(self):
        self.text = ''
        return self.success('geleert')

    # --- Ausgabe ------------------------------------------------------------------------------

    @op("Gibt text als Morse-Ton auf dem Ausgang device aus (im Hintergrund)",
        result={'success': 'bool', 'message': 'str', 'morse': 'str'})
    def play(self, text: str = 'SOS', wpm: float = 20, freq: float = 600, device: str = ''):
        import threading
        morse = text_to_morse(str(text)[:Morse.MAX_TEXT])
        if not morse:
            return self.fail('Kein morsbarer Text')
        if self.playing:
            return self.fail('Ausgabe laeuft bereits')
        if not (5 <= wpm <= 60) or not (100 <= freq <= 4000):
            return self.fail('wpm muss zwischen 5 und 60, freq zwischen 100 und 4000 Hz liegen')
        self.playing = True
        threading.Thread(target=self.play_loop, args=(morse, wpm, freq, device or self.out_device),
                         name='morse_play', daemon=True).start()
        return self.success('Ausgabe: ' + morse, morse=morse)

    @op("Bricht die Ausgabe ab", result={'success': 'bool', 'message': 'str'})
    def stop_play(self):
        self.playing = False
        return self.success('Ausgabe abgebrochen')

    def play_loop(self, morse: str, wpm: float, freq: float, device):
        """ Ausgabe-Thread: Callback-Stream (geht mit allen PortAudio-Backends) """
        import threading
        import sounddevice as sd
        try:
            device = self.resolve_device(sd, device, 'output')
            rate, channels = self.find_settings(sd, device, 'output')
            data = self.out_bytes(synth_pcm(morse, wpm, freq, rate, padding=0.2), channels)
            done, pos = threading.Event(), [0]

            def callback(outdata, frames, t, status):
                chunk = data[pos[0]:pos[0] + len(outdata)] if self.playing else b''
                pos[0] += len(outdata)
                outdata[:len(chunk)] = chunk
                outdata[len(chunk):] = bytes(len(outdata) - len(chunk))
                if len(chunk) < len(outdata):
                    raise sd.CallbackStop

            with sd.RawOutputStream(samplerate=rate, device=device, channels=channels, dtype='int16',
                                    callback=callback, finished_callback=done.set):
                done.wait(len(data) / 2 / channels / rate + 5)
            self.info(f'Morse-Ausgabe: {morse} ({rate} Hz, {channels} Kanal/Kanaele)')
        except Exception as e:
            self.error_msg = str(e)
            self.error(f'Morse-Ausgabe: {e}')
        self.playing = False

    @staticmethod
    def out_bytes(pcm, channels: int) -> bytes:
        """ Mono-PCM auf channels Kanaele verteilen, als int16-Bytes (little endian) """
        if channels > 1:
            res = array.array('h', bytes(len(pcm) * 2 * channels))
            for c in range(channels):
                res[c::channels] = pcm
            pcm = res
        if sys.byteorder == 'big':
            pcm.byteswap()
        return pcm.tobytes()

    # --- Seiten -------------------------------------------------------------------------------

    def page_status(self, params={}):
        from flask import jsonify
        return jsonify(self.status())

    def part_index(self, p: b.Page, params={}):
        p.script(MORSE_JS)
        p('<style>' + MORSE_CSS + '</style>')
        self.part_listen(p)
        self.part_play(p)
        with p.section(h="Ergebnis"):
            p.pre('', id='result')
        p.script(f'morse_poll({json_str(self.dispatcher_link(self.page_status))});')

    def part_listen(self, p: b.Page):
        with p.section(h="Empfang (Eingang → Text)"):
            p.form_select('morse_in', "Eingang", self.device_options('in', self.device), value=str(self.device or ''),
                          id='morse_in', hint="Eingangsgerät, z.B. USB-Soundkarte")
            p.form_input('morse_rfreq', "Filter (Hz)", id='morse_rfreq', type='number', value=f'{self.freq:g}',
                         min='0', hint="Tonhöhe für den Goertzel-Filter gegen Störungen, 0 = nur Lautstärke")
            if self.running:
                p(self.action_btn(dict(type=self.type, op='stop'), label="Stop", hint="Empfang beenden"))
            else:
                p(self.action_btn_parametric("Start", dict(type=self.type, op='listen', device='#morse_in',
                                                           freq='#morse_rfreq'), hint="Empfang starten"))
            p(self.action_btn(dict(type=self.type, op='clear'), label="Leeren", hint="Empfangenen Text leeren"))
            p.div('', id='morse_rx_info', _class='morse_hint')
            p.div('', id='morse_rx_text', _class='morse_out morse_rx')

    def part_play(self, p: b.Page):
        with p.section(h="Ausgabe (Text → Ausgang)"):
            p.form_input('morse_ptext', "Text", id='morse_ptext', value='CQ CQ DE NX', hint="Zu sendender Text")
            p.form_input('morse_pwpm', "Tempo (WPM)", id='morse_pwpm', type='number', value='20', min='5', max='60',
                         hint="Wörter pro Minute (PARIS), 5 bis 60")
            p.form_input('morse_pfreq', "Ton (Hz)", id='morse_pfreq', type='number', value='600', min='100',
                         max='4000', hint="Tonhöhe in Hz, 100 bis 4000")
            p.form_select('morse_out', "Ausgang", self.device_options('out', self.out_device), value=str(self.out_device or ''),
                          id='morse_out', hint="Ausgangsgerät, z.B. USB-Soundkarte")
            p(self.action_btn_parametric("Abspielen", dict(type=self.type, op='play', text='#morse_ptext',
                                                           wpm='#morse_pwpm', freq='#morse_pfreq',
                                                           device='#morse_out'),
                                         hint="Text als Morse-Ton ausgeben"))
            p(self.action_btn(dict(type=self.type, op='stop_play'), label="Abbrechen", hint="Ausgabe abbrechen"))


def json_str(value: str) -> str:
    """ JS-String-Literal (fuer Script-Code, nicht fuer HTML-Attribute) """
    import json
    return json.dumps(value).replace('</', '<\\/')


# language=css
MORSE_CSS = """
    .morse_out { margin: 8px 0; font-size: 1.2em; }
    .morse_out code { font-size: 1.3em; letter-spacing: 2px; white-space: normal; overflow-wrap: anywhere; }
    .morse_hint { color: #888; font-size: 0.9em; margin-bottom: 6px; }
    .morse_file { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin: 4px 0; }
    .morse_name { flex: 1 1 220px; min-width: 0; overflow-wrap: anywhere; font-family: monospace; }
    .morse_file audio { max-width: 100%; }
    .morse_rx { font-family: monospace; min-height: 3em; white-space: pre-wrap; overflow-wrap: anywhere; }
"""

# language=js
MORSE_JS = """
    window.morse_esc = function(s) {
        const d = document.createElement('div'); d.textContent = s == null ? '' : String(s); return d.innerHTML;
    }
    window.morse_encoded = function(res) {
        const el = document.getElementById('morse_player');
        if (!res.success) { el.innerHTML = morse_esc(res.message); return; }
        let h = '<div><code>' + morse_esc(res.morse) + '</code></div>';
        if (res.url) {
            h += '<audio controls autoplay src="' + morse_esc(res.url) + '"></audio> ';
            h += '<a href="' + morse_esc(res.url + '&download=1') + '" title="MP3 herunterladen">Download</a>';
        } else {
            h += '<div>' + morse_esc(res.file) + '</div>';
        }
        el.innerHTML = h;
    }
    window.morse_converted = function(res) {
        const v = res.hasOwnProperty('morse') ? res.morse : res.text;
        document.getElementById('morse_chars').value = v;
    }
    window.morse_upload = function(url) {
        const input = document.getElementById('morse_upload');
        const el = document.getElementById('morse_decoded');
        if (!input.files.length) { el.innerHTML = 'Bitte eine Datei auswählen'; return; }
        const fd = new FormData();
        fd.append('audio', input.files[0]);
        fd.append('freq', document.getElementById('morse_dfreq').value || '0');
        el.innerHTML = 'Decodiere...';
        fetch(url, {method: 'POST', body: fd}).then(r => r.json()).then(res => {
            document.getElementById('result').innerHTML = morse_esc(JSON.stringify(res, null, 2));
            if (!res.success) { el.innerHTML = morse_esc(res.message); return; }
            el.innerHTML = '<div><b>' + morse_esc(res.text) + '</b> (' + morse_esc(res.wpm) + ' WPM)</div>' +
                '<div><code>' + morse_esc(res.morse) + '</code></div>' +
                '<audio controls src="' + morse_esc(res.url) + '"></audio>';
        }).catch(e => { el.innerHTML = morse_esc(e); });
    }
    window.morse_poll = function(url) {
        if (window.morse_poll_timer) clearInterval(window.morse_poll_timer);
        const update = () => {
            const text = document.getElementById('morse_rx_text');
            if (!text) { clearInterval(window.morse_poll_timer); return; }
            fetch(url).then(r => r.json()).then(s => {
                text.textContent = s.text || '';
                document.getElementById('morse_rx_info').textContent = s.message +
                    (s.running ? ' | ' + s.wpm + ' WPM | Pegel ' + s.level + ' / Schwelle ' + (s.threshold || '-') : '') +
                    (s.playing ? ' | Ausgabe läuft' : '');
            }).catch(() => {});
        };
        update();
        window.morse_poll_timer = setInterval(update, 1000);
    }
"""
