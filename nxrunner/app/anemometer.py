"""
Kalibrierung eines Heizdraht-Anemometers und Auswertung mit dem trainierten Modell.

AnemometerKalibrierung, benoetigte Runner:
    m5        EspM5 (nxrunner.devices.esp_m5) mit AS5600, misst die Drehzahl der Scheibe
    esphome   EspHomeDevice mit dem Stepper, der die Scheibe dreht (stepper.yml: Switch "Stepper A Dauerlauf",
              Number "Stepper Max Speed")
    ads       ADC mit 3 analogen Signalen (Kanal channel bis channel+2), erkannt an der Signatur
              {"read_a": 1, "channel": n} -> {"value": ...}, z.B. nxbot.i2c.ADS oder ein EspHomeDevice
              mit den Entities analog_a0, analog_a1 und analog_a2

    Ablauf (page=calibration): mehrere Laeufe, jeder beginnt im Stillstand und steigert die Stepper-Geschwindigkeit
    langsam von speed_min bis speed_max. Je Stufe wird settle s gewartet und measure s lang gemessen: Drehzahl
    (-> Geschwindigkeit des Sensors auf der Scheibe, Radius distance) und A0-A2. Die Messwerte werden als CSV in path
    gespeichert. Daraus wird je Kanal eine Kennlinie nach King's Law (E² = A + B·vⁿ) auf den Mittelwerten je Stufe
    trainiert (Referenz: Stepper-Geschwindigkeit oder Drehzahl), Kanaele mit zu wenig Signal ueber dem Rauschen
    fallen weg. Die Fehlerquote zusaetzlich per Kreuzvalidierung ueber die Laeufe (ein Lauf wird jeweils mit den
    anderen vorhergesagt). Das Modell (model.json) nutzt der Runner Anemometer.

    Config (runners):
        anemometer: nxrunner.app.anemometer:AnemometerKalibrierung()
        anemometer: nxrunner.app.anemometer:AnemometerKalibrierung(m5="m5_multi", esphome="stepper3", ads="esp-anemometer")
        anemometer: nxrunner.app.anemometer:AnemometerKalibrierung(path="~/.config/nxrunner/anemometer", distance=15)

    Jobs:
        {"type": "anemometer", "op": "set_runners", "m5": "m5_multi", "esphome": "stepper3", "ads": "esp-anemometer"}
        {"type": "anemometer", "op": "start_calibration", "runs": 3, "speed_min": 20, "speed_max": 250, "speed_step": 20}
        {"type": "anemometer", "op": "stop_calibration"}
        {"type": "anemometer", "op": "progress", "since": 0}         Status und Messwerte ab Index since
        {"type": "anemometer", "op": "load_samples", "name": "kalibrierung_20261108_120000.csv"}
        {"type": "anemometer", "op": "train", "reference": "stepper", "min_snr": 3}
        {"type": "anemometer", "op": "create_anemometer", "name": "wind"}

Anemometer: wertet A0-A2 laufend mit dem Modell aus

    Config (runners):
        wind: nxrunner.app.anemometer:Anemometer(ads="esp-anemometer")
        wind: nxrunner.app.anemometer:Anemometer(ads="esp-anemometer", model="~/.config/nxrunner/anemometer/model.json", interval=1)

    Variablen: speed (m/s), speed_kmh, raw_speed (ungeglaettet), in_range (False = ueber dem kalibrierten Bereich,
    Wert per Kennlinie hochgerechnet), a0, a1, a2, last_update, error
    Jobs:
        {"type": "wind", "op": "get_wind"}        aktuelle Geschwindigkeit
        {"type": "wind", "op": "reload_model"}
        {"type": "wind", "op": "start_measure"} / {"op": "stop_measure"}
"""
import csv
import html
import json
import math
import time
from collections import deque
from pathlib import Path
from threading import Thread

from nwebclient import base
from nwebclient.base import category
from nwebclient import runner as r
from nwebclient import web as w
from nwebclient.runner import op

from nxrunner.devices.esp_m5 import EspM5


DEFAULT_PATH = '~/.config/nxrunner/anemometer'

CHART_JS = 'https://cdn.jsdelivr.net/npm/chart.js'

COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']


# --- ADC ----------------------------------------------------------------------------------------

def is_adc(runner) -> bool:
    """ Runner mit der Signatur {"read_a": 1, "channel": n} """
    return any('read_a' in s and 'channel' in s for s in getattr(runner, 'signatures', []))


def read_analog(ads, channel):
    """ Liest einen Kanal ueber den Job read_a (Signatur), damit funktionieren ADS und EspHomeDevice gleich """
    res = ads.execute({'type': ads.type, 'read_a': 1, 'channel': channel})
    if not res.get('success', False):
        raise ValueError(res.get('message', 'read_a fehlgeschlagen'))
    return float(res['value'])


def ensure_live(ads, timeout=15):
    """ EspHomeDevice: Live-Verbindung, damit read_a nicht fuer jeden Wert neu verbindet """
    if not hasattr(ads, 'start_live') or getattr(ads, 'live_api', None) is not None:
        return
    ads.start_live()
    end = time.time() + timeout
    while ads.live_api is None and time.time() < end:
        time.sleep(0.2)


# --- Diagramme (Chart.js) -----------------------------------------------------------------------

def add_chart(p: base.Page, chart_id: str, cfg: dict):
    """ Diagramm, erreichbar im Browser als window.anemoCharts[chart_id] """
    p.script(CHART_JS)
    p(f'<div style="max-width: 900px"><canvas id="{chart_id}"></canvas></div>')
    p.js_ready(f'window.anemoCharts = window.anemoCharts || {{}}; '
               f'window.anemoCharts["{chart_id}"] = new Chart(document.getElementById("{chart_id}"), {json.dumps(cfg)});')


def scatter_cfg(datasets: list, x_title: str, y_title: str, y2_title: str = None) -> dict:
    """ :param datasets: [{'label', 'data': [{'x', 'y'}], 'line': bool, 'y2': bool, 'color': optional}] """
    sets = []
    for i, ds in enumerate(datasets):
        line = ds.get('line', False)
        color = ds.get('color', COLORS[i % len(COLORS)])
        sets.append({'label': ds['label'], 'data': ds['data'], 'borderColor': color,
                     'backgroundColor': color, 'showLine': line, 'pointRadius': 0 if line else 2,
                     'borderWidth': 1.5, 'yAxisID': 'y2' if ds.get('y2') else 'y'})
    scales = {'x': {'type': 'linear', 'title': {'display': True, 'text': x_title}},
              'y': {'title': {'display': True, 'text': y_title}}}
    if y2_title:
        scales['y2'] = {'position': 'right', 'grid': {'drawOnChartArea': False},
                        'title': {'display': True, 'text': y2_title}}
    return {'type': 'scatter', 'data': {'datasets': sets},
            'options': {'animation': False, 'scales': scales}}


# --- Modell -------------------------------------------------------------------------------------

KEYS = ['a0', 'a1', 'a2']

# Referenz fuer die Geschwindigkeit beim Training: Name -> Titel
REFERENCES = {
    'stepper': "Stepper (gleichmäßig, Drehzahl daraus hochgerechnet)",
    'rpm': "Drehzahl AS5600 (Mittel je Stufe)",
}


def unique_samples(samples) -> list:
    """ Messwerte ohne Wiederholungen: der ADC liefert seltener neue Werte, als abgefragt wird """
    res, prev = [], None
    for s in samples:
        values = tuple(s[k] for k in KEYS)
        if values != prev:
            res.append(s)
        prev = values
    return res


def step_means(samples, reference='stepper') -> list:
    """
    Mittelwerte je Lauf und Stufe (ohne wiederholte ADC-Werte) als Trainingsdaten.
    reference=stepper: v aus der Stepper-Geschwindigkeit, Faktor rpm je step/s per kleinste Quadrate ueber alle
    Messwerte. Die Drehzahl des AS5600 schwankt staerker als der Stepper.
    """
    moving = [s for s in samples if s['speed'] > 0 and s['rpm'] > 0]
    rpm_per_step = sum(s['rpm'] * s['speed'] for s in moving) / sum(s['speed'] ** 2 for s in moving) if moving else 0
    v_per_rpm = next((s['v'] / s['rpm'] for s in moving if s['v'] > 0), 0)
    groups = {}
    for s in unique_samples(samples):
        groups.setdefault((s['run'], s['speed']), []).append(s)
    steps = []
    for (run, speed), items in sorted(groups.items()):
        rpm = sum(s['rpm'] for s in items) / len(items)
        v = speed * rpm_per_step * v_per_rpm if reference == 'stepper' else sum(s['v'] for s in items) / len(items)
        steps.append({'run': run, 'speed': speed, 'n': len(items), 'rpm': rpm, 'v': v,
                      **{k: sum(s[k] for s in items) / len(items) for k in KEYS}})
    return steps


class AnemometerModel:
    """
    King's Law je Kanal: E² = A + B·vⁿ (E = Signal des Heizdrahts, v in m/s).
    n per Gittersuche 0,2-1,0, A und B per kleinste Quadrate; Umkehrung v = ((E² - A) / B)^(1/n).
    Die Kennlinie ist monoton, ausserhalb des kalibrierten Bereichs wird sie fortgesetzt statt umzuknicken.
    Kanaele, deren Aenderung ueber den Bereich kleiner als min_snr mal das Rauschen ist, werden nicht verwendet,
    die uebrigen nach SNR² gewichtet gemittelt.
    """

    KIND = 'kings_law'
    KEYS = KEYS

    def __init__(self, channels=None, metrics=None, info=None):
        self.channels = channels or {}  # Kanal -> {a, b, n, snr, weight, used, e_min, e_max}
        self.metrics = metrics or {}
        self.info = info or {}

    # --- Training ---

    @staticmethod
    def fit_channel(e, v) -> dict:
        """ King's Law fuer einen Kanal, SNR = Aenderung von E² ueber den Bereich / Streuung um die Kennlinie """
        import numpy as np
        e2, best = np.asarray(e) ** 2, None
        for n in np.arange(0.2, 1.001, 0.05):
            F = np.column_stack([np.ones(len(v)), np.asarray(v) ** n])
            (a, b), *_ = np.linalg.lstsq(F, e2, rcond=None)
            sd = float(np.sqrt(((F @ [a, b] - e2) ** 2).mean()))
            if best is None or sd < best['sd']:
                best = {'a': float(a), 'b': float(b), 'n': round(float(n), 2), 'sd': sd}
        span = abs(best['b']) * (max(v) ** best['n'] - min(v) ** best['n'])
        best['snr'] = span / best['sd'] if best['sd'] > 0 else float('inf')
        best.update(e_min=float(min(e)), e_max=float(max(e)))
        return best

    @classmethod
    def fit_steps(cls, steps, min_snr=3.0) -> "AnemometerModel":
        v = [s['v'] for s in steps]
        channels = {k: cls.fit_channel([s[k] for s in steps], v) for k in KEYS}
        used = [k for k, c in channels.items() if c['snr'] >= min_snr and c['b'] != 0]
        if not used:  # kein Kanal reicht, wenigstens den besten verwenden
            used = [max(channels, key=lambda k: channels[k]['snr'])]
        total = sum(channels[k]['snr'] ** 2 for k in used)
        for k, c in channels.items():
            c['used'] = k in used
            c['weight'] = c['snr'] ** 2 / total if k in used and total > 0 else (1.0 if k in used else 0.0)
        return cls(channels, info={'v_min': min(v), 'v_max': max(v), 'min_snr': min_snr})

    @classmethod
    def fit(cls, steps, min_snr=3.0, info=None) -> "AnemometerModel":
        model = cls.fit_steps(steps, min_snr)
        model.info.update(info or {}, steps=len(steps), runs=len({s['run'] for s in steps}),
                          created=time.strftime('%Y-%m-%d %H:%M:%S'))
        model.metrics = model.evaluate(steps)
        return model

    # --- Vorhersage ---

    def predict_channel(self, k, e) -> float:
        c = self.channels[k]
        x = (e * e - c['a']) / c['b']
        return x ** (1 / c['n']) if x > 0 else 0.0

    def predict(self, values) -> float:
        """ :param values: Signale in der Reihenfolge KEYS """
        e = dict(zip(KEYS, values))
        return sum(c['weight'] * self.predict_channel(k, e[k]) for k, c in self.channels.items() if c['used'])

    def predict_array(self, X):
        import numpy as np
        return np.array([self.predict(row) for row in X])

    def in_range(self, v) -> bool:
        """ Geschwindigkeit innerhalb des kalibrierten Bereichs (10 % Reserve) """
        return v <= self.info.get('v_max', 0) * 1.1

    def used_keys(self) -> list:
        return [k for k, c in self.channels.items() if c['used']]

    # --- Fehler ---

    @staticmethod
    def arrays(steps):
        import numpy as np
        return np.array([[float(s[k]) for k in KEYS] for s in steps]), np.array([float(s['v']) for s in steps])

    @staticmethod
    def errors(y, pred) -> dict:
        """ RMSE, MAE, max. Fehler in m/s, R² und mittlerer relativer Fehler (v > 20 % von v max) in % """
        import numpy as np
        err = pred - y
        ss_tot = float(((y - y.mean()) ** 2).sum())
        moving = y > 0.2 * y.max()
        rel = float((np.abs(err[moving]) / y[moving]).mean() * 100) if moving.any() else None
        return {'rmse': float(np.sqrt((err ** 2).mean())), 'mae': float(np.abs(err).mean()),
                'max': float(np.abs(err).max()), 'r2': 1 - float((err ** 2).sum()) / ss_tot if ss_tot > 0 else None,
                'rel_pct': rel}

    def cross_validate(self, steps):
        """ :return: Vorhersage je Stufe, jeder Lauf mit einem Modell aus den anderen Laeufen, None bei 1 Lauf """
        import numpy as np
        runs = sorted({s['run'] for s in steps})
        if len(runs) < 2:
            return None
        pred = np.zeros(len(steps))
        for run in runs:
            model = self.fit_steps([s for s in steps if s['run'] != run], self.info.get('min_snr', 3.0))
            for i, s in enumerate(steps):
                if s['run'] == run:
                    pred[i] = model.predict([s[k] for k in KEYS])
        return pred

    def evaluate(self, steps) -> dict:
        X, y = self.arrays(steps)
        res = {'train': self.errors(y, self.predict_array(X))}
        cv = self.cross_validate(steps)
        if cv is not None:
            res['cv'] = self.errors(y, cv)
        return res

    # --- Speichern ---

    def to_dict(self) -> dict:
        return {'kind': self.KIND, 'keys': KEYS, 'channels': self.channels, 'metrics': self.metrics, 'info': self.info}

    @classmethod
    def from_dict(cls, data: dict) -> "AnemometerModel":
        if data.get('kind') != cls.KIND:
            raise ValueError('Altes Modellformat (Polynom), bitte neu trainieren')
        return cls(data['channels'], data.get('metrics'), data.get('info'))

    def save(self, file):
        Path(file).parent.mkdir(parents=True, exist_ok=True)
        Path(file).write_text(json.dumps(self.to_dict(), indent=2), encoding='utf-8')

    @classmethod
    def load(cls, file) -> "AnemometerModel":
        return cls.from_dict(json.loads(Path(file).read_text(encoding='utf-8')))

    def describe(self) -> str:
        info = self.info
        return (f"King's Law, Kanäle {', '.join(k.upper() for k in self.used_keys())}, {info.get('steps')} Stufen aus "
                f"{info.get('runs')} Läufen, Referenz {info.get('reference', '–')}, {info.get('created')}, "
                f"kalibriert bis {info.get('v_max', 0):.3f} m/s")


def metrics_rows(metrics: dict) -> list:
    """ Zeilen fuer p.grid: Kennzahl, Training, Kreuzvalidierung """
    titles = {'rmse': "RMSE (m/s)", 'mae': "Mittlerer Fehler (m/s)", 'max': "Max. Fehler (m/s)", 'r2': "R²",
              'rel_pct': "Fehlerquote (%, v > 20 % von v max)"}
    fmt = lambda v: '–' if v is None else f'{v:.3f}'
    return [{'name': title, 'train': fmt(metrics.get('train', {}).get(k)), 'cv': fmt(metrics.get('cv', {}).get(k))}
            for k, title in titles.items()]


def part_metrics(p: base.Page, metrics: dict):
    p.grid(metrics_rows(metrics), [w.Grid.col('name', "Kennzahl"), w.Grid.col('train', "Training"),
                                   w.Grid.col('cv', "Kreuzvalidierung (je Lauf)")])


def channel_rows(model: AnemometerModel) -> list:
    """ Kennlinie je Kanal fuer p.grid """
    return [{'ch': k.upper(), 'a': f"{c['a']:.5f}", 'b': f"{c['b']:+.5f}", 'n': f"{c['n']:.2f}",
             'snr': f"{c['snr']:.1f}", 'weight': f"{c['weight'] * 100:.0f} %" if c['used'] else 'nicht verwendet'}
            for k, c in model.channels.items()]


def part_channels(p: base.Page, model: AnemometerModel):
    p.grid(channel_rows(model), [w.Grid.col('ch', "Kanal"), w.Grid.col('a', "A"), w.Grid.col('b', "B"),
                                 w.Grid.col('n', "n"), w.Grid.col('snr', "Signal/Rauschen"),
                                 w.Grid.col('weight', "Gewicht")])
    if not any(c['snr'] >= model.info.get('min_snr', 3.0) for c in model.channels.values()):
        p.alert_warn("Kein Kanal ändert sich deutlich über dem Rauschen. Die Scheibe dreht vermutlich zu langsam "
                     "für den Heizdraht, Kalibrierung mit höheren Geschwindigkeiten wiederholen.")


# --- Kalibrierung -------------------------------------------------------------------------------

class AnemometerKalibrierung(r.BaseJobExecutor):

    TAGS = ['app', 'anemometer']

    MODULES = ['numpy']

    # Rolle -> (Titel, Test ob ein Runner passt)
    ROLES = {
        'm5': ("EspM5 (Drehzahl der Scheibe)", lambda x: isinstance(x, EspM5)),
        'esphome': ("EspHomeDevice (Stepper)", lambda x: any(c.__name__ == 'EspHomeDevice' for c in type(x).__mro__)),
        'ads': ("ADC, 3 analoge Signale (read_a)", is_adc),
    }

    """ Anzahl der analogen Signale des Anemometers """
    CHANNELS = 3

    # Einstellungen eines Kalibrierlaufs: Name -> (Titel, Standardwert)
    SETTINGS = {
        'runs': ("Läufe", 3),
        'speed_min': ("Start (steps/s)", 20),
        'speed_max': ("Ende (steps/s)", 250),
        'speed_step': ("Schrittweite (steps/s)", 20),
        'settle': ("Einschwingen je Stufe (s)", 8),
        'measure': ("Messen je Stufe (s)", 12),
        'interval': ("Messintervall (s)", 1),
        'pause': ("Pause zwischen Läufen (s)", 20),
    }

    def __init__(self, type='anemometer', m5=None, esphome=None, ads=None, channel=0, distance=15, path=DEFAULT_PATH,
                 speed_entity='stepper_max_speed', run_entity='stepper_a_dauerlauf'):
        """
        :param m5: Name des EspM5-Runners, ohne Angabe der erste passende
        :param esphome: Name des EspHomeDevice-Runners mit dem Stepper, ohne Angabe der erste passende
        :param ads: Name des Runners mit der Signatur read_a, ohne Angabe der erste passende
        :param channel: erster der 3 Kanaele des ADC fuer das Anemometer
        :param distance: Abstand des Sensors zur Drehachse (Radius) in cm
        :param path: Verzeichnis fuer Messwerte (CSV) und Modell (model.json)
        :param speed_entity: Number des Steppers fuer die Geschwindigkeit (steps/s)
        :param run_entity: Switch des Steppers fuer den Dauerlauf
        """
        super().__init__(type)
        self.m5 = m5
        self.esphome = esphome
        self.ads = ads
        self.channel = int(channel)
        self.distance = float(distance)
        self.path = Path(path).expanduser()
        self.speed_entity = speed_entity
        self.run_entity = run_entity
        self.settings = {k: v for k, (_, v) in self.SETTINGS.items()}
        self.samples = []
        self.samples_file = None
        self.status = 'Bereit'
        self.stopping = False
        self.worker = None
        self.t0 = time.time()
        self.model = self.load_model()
        self.define_vars('distance', 'status')

    # --- Runner -------------------------------------------------------------------------------

    def executor(self):
        return self.getRoot().jobexecutor

    def runner_by_name(self, name):
        get_runner = getattr(self.executor(), 'get_runner', None)
        return get_runner(name) if name and get_runner is not None else None

    def matches(self, role, runner) -> bool:
        """ Stepper: nur EspHomeDevices mit dem Switch run_entity (sofern die Entities schon geladen sind) """
        if not self.ROLES[role][1](runner):
            return False
        entities = getattr(runner, 'entities', None) if role == 'esphome' else None
        return not entities or any(e.object_id == self.run_entity for e in entities)

    def candidates(self, role) -> dict:
        """ :return: {Name: Runner} der geladenen Runner, die fuer die Rolle passen """
        test = lambda x: self.matches(role, x)
        res = {}
        for runner in self.executor():
            if runner is not self and test(runner):
                res[runner.type] = runner
        selected = self.runner_by_name(getattr(self, role))
        if selected is not None and test(selected):
            res[selected.type] = selected
        return res

    def runner(self, role):
        """ :return: gewaehlter bzw. erster passender Runner, None wenn keiner vorhanden """
        candidates = self.candidates(role)
        name = getattr(self, role)
        if name:
            return candidates.get(name)
        return next(iter(candidates.values()), None)

    def missing(self) -> list:
        """ :return: Titel der Rollen ohne Runner """
        return [title for role, (title, _) in self.ROLES.items() if self.runner(role) is None]

    @op("Waehlt die Runner fuer m5, esphome und ads (leer = erster passender)", result={'success': 'bool', 'message': 'str'})
    def set_runners(self, m5: str = '', esphome: str = '', ads: str = ''):
        self.m5 = m5 or None
        self.esphome = esphome or None
        self.ads = ads or None
        missing = self.missing()
        if missing:
            return self.fail('Fehlt: ' + ', '.join(missing))
        return self.success('Alle Runner vorhanden')

    # --- Messung ------------------------------------------------------------------------------

    def channels(self) -> list:
        return [self.channel + i for i in range(self.CHANNELS)]

    def read_analog(self, ads, channel):
        return read_analog(ads, channel)

    def set_distance(self, value):
        """ Setter fuer set_var (UI liefert Strings), z.B. "12,5" -> 12.5 """
        self.distance = float(str(value).replace(',', '.'))

    def get_speed(self, rpm):
        """ Geschwindigkeit in m/s des Sensors auf der Scheibe, Umfang bei distance (cm) mal Umdrehungen je Sekunde """
        return 2 * math.pi * float(self.distance) / 100 * float(rpm) / 60

    def read_rpm(self) -> float:
        m5 = self.runner('m5')
        res = m5.execute({'type': m5.type, 'op': 'get_rotation'})
        if not res.get('success', False):
            raise ValueError(f"{m5.type}: {res.get('message', 'get_rotation fehlgeschlagen')}")
        return abs(float(res['value'].get('rpm') or 0))

    def sample(self, run, speed) -> dict:
        """ Ein Messwert: Zeit, Lauf, Stepper-Geschwindigkeit, Drehzahl, Geschwindigkeit und A0-A2 """
        rpm = self.read_rpm()
        ads = self.runner('ads')
        values = {f'a{i}': self.read_analog(ads, ch) for i, ch in enumerate(self.channels())}
        s = {'t': round(time.time() - self.t0, 2), 'run': run, 'speed': speed, 'rpm': rpm,
             'v': round(self.get_speed(rpm), 4), **values}
        self.samples.append(s)
        return s

    def set_stepper(self, speed, attempts=3):
        """
        speed in steps/s, 0 haelt den Stepper an. Ueber die Live-Verbindung (sonst je Kommando eine neue API-Sitzung),
        bei Fehlern bis zu attempts Versuche
        """
        esphome = self.runner('esphome')
        for attempt in range(1, attempts + 1):
            try:
                ensure_live(esphome)
                if speed <= 0:
                    esphome.command(self.run_entity, state=False)
                else:
                    esphome.command(self.speed_entity, value=speed)
                    esphome.command(self.run_entity, state=True)
                return
            except Exception as e:
                if attempt == attempts:
                    raise RuntimeError(f'{esphome.type} reagiert nicht ({attempts} Versuche): {type(e).__name__}: {e}')
                self.error(f'{esphome.type}, Versuch {attempt}: {type(e).__name__}: {e}')
                time.sleep(2)

    # --- Ablauf -------------------------------------------------------------------------------

    def speed_steps(self) -> list:
        """ Stillstand, dann von speed_min bis speed_max in Schritten von speed_step """
        s = self.settings
        steps = [0.0]
        speed = float(s['speed_min'])
        while speed <= float(s['speed_max']) + 1e-9:
            steps.append(round(speed, 3))
            speed += max(float(s['speed_step']), 1)
        return steps

    def duration(self) -> float:
        """ Geschaetzte Dauer der Kalibrierung in s """
        s = self.settings
        per_run = len(self.speed_steps()) * (float(s['settle']) + float(s['measure']))
        return int(s['runs']) * per_run + (int(s['runs']) - 1) * float(s['pause'])

    def is_running(self) -> bool:
        return self.worker is not None and self.worker.is_alive()

    def wait(self, seconds) -> bool:
        """ Wartet, :return: False wenn die Kalibrierung abgebrochen wurde """
        end = time.time() + float(seconds)
        while time.time() < end:
            if self.stopping:
                return False
            time.sleep(0.2)
        return not self.stopping

    def measure_step(self, run, speed):
        end = time.time() + float(self.settings['measure'])
        while time.time() < end and not self.stopping:
            started = time.time()
            self.sample(run, speed)
            time.sleep(max(0.0, float(self.settings['interval']) - (time.time() - started)))

    def run_step(self, run, speed) -> bool:
        runs = int(self.settings['runs'])
        self.set_stepper(speed)
        self.status = f'Lauf {run}/{runs}, {speed:g} steps/s: einschwingen'
        if not self.wait(self.settings['settle']):
            return False
        self.status = f'Lauf {run}/{runs}, {speed:g} steps/s: messen'
        self.measure_step(run, speed)
        return not self.stopping

    def run_calibration(self):
        runs = int(self.settings['runs'])
        try:
            self.status = 'Verbinde ADC'
            ensure_live(self.runner('ads'))
            for run in range(1, runs + 1):
                for speed in self.speed_steps():
                    if not self.run_step(run, speed):
                        self.status = 'Abgebrochen'
                        return
                self.set_stepper(0)
                if run < runs:
                    self.status = f'Pause nach Lauf {run}/{runs}'
                    if not self.wait(self.settings['pause']):
                        self.status = 'Abgebrochen'
                        return
            self.status = f'Fertig, {len(self.samples)} Messwerte'
        except Exception as e:
            self.status = f'Fehler: {type(e).__name__}: {e}'
            self.error(e)
        finally:
            self.finish_calibration()

    def finish_calibration(self):
        try:
            self.set_stepper(0)
        except Exception as e:
            self.error(f'Stepper anhalten: {e}')
        if len(self.samples) > 0:
            self.save_samples()

    @op("Startet die Kalibrierung: runs Laeufe, Stepper von speed_min bis speed_max (steps/s) in Schritten von speed_step",
        result={'success': 'bool', 'message': 'str'})
    def start_calibration(self, runs: int = 3, speed_min: float = 20, speed_max: float = 250, speed_step: float = 20,
                          settle: float = 8, measure: float = 12, interval: float = 1, pause: float = 20):
        if self.is_running():
            return self.fail('Kalibrierung läuft bereits')
        missing = self.missing()
        if missing:
            return self.fail('Fehlt: ' + ', '.join(missing))
        self.settings = dict(runs=max(1, runs), speed_min=max(10.0, speed_min), speed_max=speed_max,
                             speed_step=speed_step, settle=settle, measure=measure, interval=max(0.2, interval),
                             pause=pause)
        self.samples = []
        self.samples_file = None
        self.stopping = False
        self.t0 = time.time()
        self.worker = Thread(target=self.run_calibration, daemon=True)
        self.worker.start()
        return self.success(f'Gestartet, Dauer ca. {self.duration() / 60:.0f} min', ui_reload=True)

    @op("Bricht die Kalibrierung ab und haelt den Stepper an", result={'success': 'bool', 'message': 'str'})
    def stop_calibration(self):
        self.stopping = True
        if not self.is_running():
            self.set_stepper(0)
        return self.success('Abbruch angefordert')

    @op("Status und Messwerte ab Index since", result={'success': 'bool', 'running': 'bool', 'status': 'str',
                                                         'count': 'int', 'samples': 'list'})
    def progress(self, since: int = 0):
        return self.success(running=self.is_running(), status=self.status, count=len(self.samples),
                            samples=self.samples[since:])

    # --- Messwerte speichern und laden --------------------------------------------------------

    FIELDS = ['t', 'run', 'speed', 'rpm', 'v', 'a0', 'a1', 'a2']

    def save_samples(self):
        self.path.mkdir(parents=True, exist_ok=True)
        file = self.path / time.strftime('kalibrierung_%Y%m%d_%H%M%S.csv')
        with open(file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.FIELDS)
            writer.writeheader()
            writer.writerows(self.samples)
        self.samples_file = file.name
        self.info(f'Messwerte gespeichert: {file}')

    def sample_files(self) -> list:
        return sorted((f.name for f in self.path.glob('kalibrierung_*.csv')), reverse=True)

    @op("Laedt gespeicherte Messwerte (CSV in path)", result={'success': 'bool', 'message': 'str'})
    def load_samples(self, name: str = ''):
        if self.is_running():
            return self.fail('Kalibrierung läuft')
        file = self.path / Path(name).name
        if not file.is_file():
            return self.fail(f'{name} nicht gefunden')
        with open(file, newline='', encoding='utf-8') as f:
            self.samples = [{k: (int(v) if k == 'run' else float(v)) for k, v in row.items()} for row in csv.DictReader(f)]
        self.samples_file = file.name
        self.status = f'{len(self.samples)} Messwerte aus {file.name}'
        return self.success(self.status, ui_reload=True)

    # --- Modell -------------------------------------------------------------------------------

    def model_file(self) -> Path:
        return self.path / 'model.json'

    def load_model(self):
        try:
            return AnemometerModel.load(self.model_file()) if self.model_file().is_file() else None
        except Exception as e:
            self.error(f'Modell laden: {e}')
            return None

    @op("Trainiert das Modell (King's Law je Kanal) aus den Mittelwerten je Stufe "
        "(reference: stepper oder rpm, min_snr: Mindest-Signal/Rauschen eines Kanals)",
        result={'success': 'bool', 'message': 'str', 'metrics': 'dict'})
    def train(self, reference: str = 'stepper', min_snr: float = 3.0):
        if self.is_running():
            return self.fail('Kalibrierung läuft')
        steps = step_means(self.samples, reference)
        if len(steps) < 5 or max((s['v'] for s in steps), default=0) <= 0:
            return self.fail('Zu wenige Stufen mit Bewegung, erst kalibrieren oder Messwerte laden')
        model = AnemometerModel.fit(steps, min_snr, info={
            'source': self.samples_file, 'distance': self.distance, 'channels': self.channels(),
            'reference': reference, 'samples': len(self.samples)})
        model.save(self.model_file())
        self.model = model
        rmse = model.metrics.get('cv', model.metrics['train'])['rmse']
        return self.success(f"Modell gespeichert, Kanäle {', '.join(model.used_keys())}, RMSE {rmse:.3f} m/s",
                            metrics=model.metrics, ui_reload=True)

    def training_steps(self) -> list:
        reference = self.model.info.get('reference', 'stepper') if self.model else 'stepper'
        return step_means(self.samples, reference)

    def step_rows(self, steps) -> list:
        """ Mittelwerte je Lauf und Stufe: Referenz, Modell, Fehler """
        rows = []
        for s in steps:
            row = {'run': s['run'], 'speed': f"{s['speed']:g}", 'n': s['n'], 'rpm': f"{s['rpm']:.1f}",
                   'v': f"{s['v']:.3f}", **{k: f"{s[k]:.4f}" for k in KEYS}}
            if self.model is not None:
                pred = self.model.predict([s[k] for k in KEYS])
                row.update(pred=f'{pred:.3f}', err=f"{pred - s['v']:+.3f}")
            rows.append(row)
        return rows

    @op("Legt einen Anemometer-Runner an, der A0-A2 laufend mit dem Modell auswertet",
        result={'success': 'bool', 'message': 'str'})
    def create_anemometer(self, name: str = 'wind'):
        if self.model is None:
            return self.fail('Kein Modell, erst trainieren')
        ads = self.runner('ads')
        with self.opt_dispatcher() as d:
            if name in d:
                return self.fail(f'Runner {name} gibt es schon')
            d.add_runner(Anemometer(name, ads=ads.type if ads else None, channel=self.channel,
                                    model=str(self.model_file())))
        return self.success(f'Runner {name} angelegt', ui_reload=True)

    # --- Seite: Runner ------------------------------------------------------------------------

    def part_index(self, p: base.Page, params={}):
        with p.section(h='Anemometer-Kalibrierung: Runner'):
            job = {'type': self.type, 'op': 'set_runners'}
            for role, (title, _) in self.ROLES.items():
                candidates = self.candidates(role)
                runner = self.runner(role)
                options = {'': '(erster passender)', **{n: f'{n} ({type(x).__name__})' for n, x in candidates.items()}}
                p.form_select(role, title, options, value=getattr(self, role) or '', id='anemo_' + role)
                if runner is None:
                    p.alert_error(f'{html.escape(title)}: kein passender Runner geladen')
                else:
                    p.div(f'Verwendet: {html.escape(runner.type)}')
                job[role] = '#anemo_' + role
            p(self.action_btn_parametric('Übernehmen', job, on_success='function(r) { location.reload(); }',
                                         hint="Verwendet die gewählten Runner für die Kalibrierung"))
        missing = self.missing()
        if missing:
            p('<button disabled title="Erst alle Runner auswählen">Kalibrierung</button>')
            p.div('Fehlt: ' + html.escape(', '.join(missing)))
        else:
            p(w.button_js('Kalibrierung', 'location.href = ' + json.dumps(self.link(self.part_calibration)) + ';',
                          hint="Öffnet die Kalibrierung mit Messläufen und Training"))
        p.pre('', id='result')
        super().part_index(p, params)

    # --- Seite: Kalibrierung ------------------------------------------------------------------

    def part_calibration(self, p: base.Page, params={}):
        missing = self.missing()
        if missing:
            p.alert_error('Fehlt: ' + html.escape(', '.join(missing)))
            p(w.a('Zurück', self.link(self.part_index)))
            return
        self.part_current(p)
        self.part_run(p)
        self.part_charts(p)
        self.part_samples(p)
        self.part_training(p)
        p.pre('', id='result')
        p(w.a('Zurück', self.link(self.part_index)))

    def part_current(self, p: base.Page):
        m5, esphome, ads = self.runner('m5'), self.runner('esphome'), self.runner('ads')
        with p.section(h='Aktuelle Werte'):
            p.div(f'EspM5: {html.escape(m5.type)}, Stepper: {html.escape(esphome.type)}, '
                  f'ADC: {html.escape(ads.type)} Kanal {self.channel}-{self.channel + self.CHANNELS - 1}, '
                  f'Radius {self.distance:g} cm')
            if self.is_running():
                p.div('Werte während der Kalibrierung siehe Diagramme.')
                return
            try:
                rpm = self.read_rpm()
                p.div(f'Drehzahl: {rpm:g} rpm, Geschwindigkeit: {self.get_speed(rpm):.2f} m/s')
            except Exception as e:
                p.alert_error(f'Drehzahl: {html.escape(type(e).__name__)}: {html.escape(str(e))}')
            for i, channel in enumerate(self.channels()):
                try:
                    p.div(f'A{channel}: {self.read_analog(ads, channel):.4f}')
                except Exception as e:
                    p.alert_error(f'A{channel}: {html.escape(type(e).__name__)}: {html.escape(str(e))}')

    def part_run(self, p: base.Page):
        with p.section(h='Messläufe'):
            p.div('Jeder Lauf beginnt im Stillstand und steigert die Geschwindigkeit des Steppers stufenweise. '
                  'Je Stufe wird erst gewartet, bis sich der Heizdraht eingeschwungen hat, dann gemessen.')
            job = {'type': self.type, 'op': 'start_calibration'}
            for key, (title, _) in self.SETTINGS.items():
                p.form_input(key, title, id='anemo_' + key, value=f'{self.settings[key]:g}')
                job[key] = '#anemo_' + key
            p.div(f'Stufen: {", ".join(f"{s:g}" for s in self.speed_steps())} steps/s, '
                  f'Dauer ca. {self.duration() / 60:.0f} min (mit den zuletzt verwendeten Einstellungen)')
            p.div(f'Status: <b id="anemo_status">{html.escape(self.status)}</b> '
                  f'(<span id="anemo_count">{len(self.samples)}</span> Messwerte)')
            if self.is_running():
                p(self.action_btn({'title': 'Abbrechen', 'type': self.type, 'op': 'stop_calibration'},
                                  hint="Bricht die Kalibrierung ab und hält den Stepper an"))
            else:
                p(self.action_btn_parametric('Kalibrierung starten', job,
                                             hint="Startet die Messläufe, der Stepper dreht die Scheibe"))
                p(self.action_btn({'title': 'Stepper anhalten', 'type': self.type, 'op': 'stop_calibration'},
                                  hint="Schaltet den Dauerlauf des Steppers aus"))

    def part_charts(self, p: base.Page):
        keys = AnemometerModel.KEYS
        with p.section(h='Messwerte über der Geschwindigkeit'):
            add_chart(p, 'anemo_scatter', scatter_cfg(
                [{'label': f'A{ch}', 'data': [{'x': s['v'], 'y': s[k]} for s in self.samples]}
                 for k, ch in zip(keys, self.channels())], "Geschwindigkeit (m/s)", "Signal"))
        with p.section(h='Verlauf'):
            add_chart(p, 'anemo_time', scatter_cfg(
                [{'label': f'A{ch}', 'line': True, 'data': [{'x': s['t'], 'y': s[k]} for s in self.samples]}
                 for k, ch in zip(keys, self.channels())] +
                [{'label': 'Geschwindigkeit (m/s)', 'line': True, 'y2': True,
                  'data': [{'x': s['t'], 'y': s['v']} for s in self.samples]}],
                "Zeit (s)", "Signal", "Geschwindigkeit (m/s)"))
        if self.is_running():
            p.js_ready(self.poll_js())

    def poll_js(self) -> str:
        """ Fragt waehrend der Kalibrierung neue Messwerte ab und haengt sie an die Diagramme an """
        job = json.dumps({'type': self.type, 'op': 'progress'})
        return '''(function() {
    var since = ''' + str(len(self.samples)) + ''';
    function add(s) {
        var sc = window.anemoCharts["anemo_scatter"], tm = window.anemoCharts["anemo_time"];
        ["a0", "a1", "a2"].forEach(function(k, i) {
            sc.data.datasets[i].data.push({x: s.v, y: s[k]});
            tm.data.datasets[i].data.push({x: s.t, y: s[k]});
        });
        tm.data.datasets[3].data.push({x: s.t, y: s.v});
    }
    function poll() {
        fetch(base_url(), post(Object.assign(''' + job + ''', {since: since}))).then(function(r) { return r.json(); })
        .then(function(d) {
            document.getElementById("anemo_status").textContent = d.status;
            document.getElementById("anemo_count").textContent = d.count;
            d.samples.forEach(add);
            since = d.count;
            window.anemoCharts["anemo_scatter"].update();
            window.anemoCharts["anemo_time"].update();
            if (d.running) { setTimeout(poll, 2000); } else { location.reload(); }
        }).catch(function() { setTimeout(poll, 5000); });
    }
    setTimeout(poll, 2000);
})();'''

    def part_samples(self, p: base.Page):
        with p.section(h='Gespeicherte Messwerte'):
            p.div(f'Aktuell: {html.escape(self.samples_file or "nicht gespeichert")}, {len(self.samples)} Messwerte, '
                  f'Verzeichnis {html.escape(str(self.path))}')
            files = self.sample_files()
            if len(files) == 0:
                p.div('Noch keine Messwerte gespeichert.')
                return
            p.form_select('file', 'Datei', files, value=self.samples_file, id='anemo_file')
            p(self.action_btn_parametric('Laden', {'type': self.type, 'op': 'load_samples', 'name': '#anemo_file'},
                                         hint="Lädt die Messwerte, z.B. um das Modell neu zu trainieren"))

    def part_training(self, p: base.Page):
        with p.section(h="Modell (King's Law)"):
            p.div("Je Kanal E² = A + B·vⁿ, trainiert auf den Mittelwerten je Lauf und Stufe (ohne wiederholte "
                  "ADC-Werte). Kanäle ohne deutliche Änderung über dem Rauschen werden nicht verwendet.")
            info = self.model.info if self.model else {}
            p.form_select('reference', 'Referenz', REFERENCES, value=info.get('reference', 'stepper'),
                          id='anemo_reference')
            p.form_input('min_snr', 'Min. Signal/Rauschen', id='anemo_min_snr', value=f"{info.get('min_snr', 3.0):g}")
            p(self.action_btn_parametric('Modell trainieren', {'type': self.type, 'op': 'train',
                                                               'reference': '#anemo_reference', 'min_snr': '#anemo_min_snr'},
                                         hint="Trainiert aus den aktuellen Messwerten A0-A2 -> Geschwindigkeit "
                                              "und speichert das Modell als model.json"))
            if self.model is None:
                p.div('Noch kein Modell trainiert.')
                return
            p.div(html.escape(self.model.describe()) + f" ({html.escape(str(info.get('source')))})")
            part_channels(p, self.model)
            part_metrics(p, self.model.metrics)
            self.part_model_chart(p)
            p.form_input('name', 'Runner-Name', id='anemo_runner_name', value='wind')
            p(self.action_btn_parametric('Anemometer-Runner anlegen',
                                         {'type': self.type, 'op': 'create_anemometer', 'name': '#anemo_runner_name'},
                                         hint="Legt einen Runner an, der A0-A2 laufend mit dem Modell auswertet"))

    def part_model_chart(self, p: base.Page):
        steps = self.training_steps()
        if len(steps) == 0:
            return
        self.part_curves(p, steps)
        X, y = AnemometerModel.arrays(steps)
        pred = self.model.predict_array(X)
        cv = self.model.cross_validate(steps)
        top = float(max(y.max(), pred.max()))
        datasets = [{'label': 'Vorhersage', 'data': [{'x': float(a), 'y': float(b)} for a, b in zip(y, pred)]}]
        if cv is not None:
            datasets.append({'label': 'Kreuzvalidierung', 'data': [{'x': float(a), 'y': float(b)} for a, b in zip(y, cv)]})
        datasets.append({'label': 'ideal', 'line': True, 'data': [{'x': 0, 'y': 0}, {'x': top, 'y': top}]})
        p.div('Vorhersage je Stufe:')
        add_chart(p, 'anemo_model', scatter_cfg(datasets, "Referenz (m/s)", "Modell (m/s)"))
        p.grid(self.step_rows(steps), [w.Grid.col('run', "Lauf"), w.Grid.col('speed', "steps/s"), w.Grid.col('n', "n"),
                                       w.Grid.col('rpm', "rpm"), w.Grid.col('v', "v Referenz"),
                                       w.Grid.col('pred', "v Modell"), w.Grid.col('err', "Fehler"),
                                       w.Grid.col('a0', "A0"), w.Grid.col('a1', "A1"), w.Grid.col('a2', "A2")])

    def part_curves(self, p: base.Page, steps):
        """ Kennlinien: Mittelwerte je Stufe und die King's-Law-Kurve je Kanal """
        v_max = max(s['v'] for s in steps)
        grid = [v_max * i / 40 for i in range(41)]
        datasets = []
        for i, (k, c) in enumerate(self.model.channels.items()):
            color = COLORS[i % len(COLORS)]
            datasets.append({'label': f'{k.upper()} Stufen', 'color': color,
                             'data': [{'x': s['v'], 'y': s[k]} for s in steps]})
            datasets.append({'label': f'{k.upper()} Kennlinie', 'line': True, 'color': color, 'data': [
                {'x': v, 'y': math.sqrt(max(c['a'] + c['b'] * v ** c['n'], 0))} for v in grid]})
        p.div('Kennlinien je Kanal:')
        add_chart(p, 'anemo_curves', scatter_cfg(datasets, "Referenz (m/s)", "Signal"))


# --- Auswertung ---------------------------------------------------------------------------------

class Anemometer(r.BaseJobExecutor):
    """ Wertet A0-A2 laufend mit dem Modell der Kalibrierung aus, Variable speed in m/s """

    TAGS = ['sensor', 'anemometer']

    MODULES = ['numpy']

    def __init__(self, type='anemometer', ads=None, channel=None, model=DEFAULT_PATH + '/model.json', interval=1.0,
                 smoothing=0.3, autostart=True):
        """
        :param ads: Name des Runners mit der Signatur read_a, ohne Angabe der erste passende
        :param channel: erster der 3 Kanaele, ohne Angabe aus dem Modell
        :param model: model.json der Kalibrierung
        :param interval: Sekunden zwischen den Messungen
        :param smoothing: Glaettung 0-1 (gleitender Mittelwert, 0 = keine)
        """
        super().__init__(type)
        self.ads = ads
        self.model_path = Path(model).expanduser()
        self.interval = float(interval)
        self.smoothing = float(smoothing)
        self.model = None
        self.error_msg = ''
        self.load()
        info_channels = (self.model.info.get('channels') if self.model else None) or [0, 1, 2]
        self.channel = int(channel) if channel is not None else int(info_channels[0])
        self.speed = None
        self.speed_kmh = None
        self.raw_speed = None
        self.in_range = None
        self.a0 = self.a1 = self.a2 = None
        self.last_update = None
        self.history = deque(maxlen=600)
        self.running = False
        self.worker = None
        self.define_vars('speed', 'speed_kmh', 'raw_speed', 'in_range', 'a0', 'a1', 'a2', 'last_update', 'interval')
        if autostart:
            self.start_measure()

    def load(self):
        try:
            self.model = AnemometerModel.load(self.model_path)
            self.error_msg = ''
        except Exception as e:
            self.model = None
            self.error_msg = f'Modell {self.model_path}: {e}'

    def adc(self):
        """ :return: Runner fuer read_a, per Name oder der erste passende """
        executor = self.getRoot().jobexecutor
        if self.ads:
            return executor.get_runner(self.ads)
        return next((x for x in executor if x is not self and is_adc(x)), None)

    def measure(self):
        ads = self.adc()
        if ads is None:
            raise ValueError('Kein ADC (read_a) gefunden')
        if self.model is None:
            raise ValueError(self.error_msg or 'Kein Modell')
        ensure_live(ads)
        values = [read_analog(ads, self.channel + i) for i in range(len(AnemometerModel.KEYS))]
        self.a0, self.a1, self.a2 = values
        self.raw_speed = round(self.model.predict(values), 3)
        prev = self.speed if self.speed is not None else self.raw_speed
        self.speed = round(self.smoothing * prev + (1 - self.smoothing) * self.raw_speed, 3)
        self.in_range = self.model.in_range(self.speed)
        self.speed_kmh = round(self.speed * 3.6, 2)
        self.last_update = time.strftime('%H:%M:%S')
        self.history.append({'t': round(time.time(), 1), 'v': self.speed, 'raw': self.raw_speed})
        self.error_msg = ''

    def loop(self):
        while self.running:
            started = time.time()
            try:
                self.measure()
            except Exception as e:
                self.error_msg = f'{type(e).__name__}: {e}'
            time.sleep(max(0.1, self.interval - (time.time() - started)))

    @op("Startet die laufende Auswertung", result={'success': 'bool', 'message': 'str'})
    def start_measure(self):
        if self.worker is not None and self.worker.is_alive():
            return self.success('läuft bereits')
        self.running = True
        self.worker = Thread(target=self.loop, daemon=True)
        self.worker.start()
        return self.success('gestartet', ui_reload=True)

    @op("Stoppt die laufende Auswertung", result={'success': 'bool', 'message': 'str'})
    def stop_measure(self):
        self.running = False
        return self.success('gestoppt', ui_reload=True)

    @op("Laedt das Modell neu (nach einer neuen Kalibrierung)", result={'success': 'bool', 'message': 'str'})
    def reload_model(self):
        self.load()
        return self.fail(self.error_msg) if self.model is None else self.success('Modell geladen', ui_reload=True)

    @op("Aktuelle Wind- bzw. Bewegungsgeschwindigkeit", result={'success': 'bool', 'speed': 'float',
                                                                'speed_kmh': 'float', 'values': 'list'})
    def get_wind(self):
        if self.speed is None:
            return self.fail(self.error_msg or 'Noch kein Messwert')
        return self.success(speed=self.speed, speed_kmh=self.speed_kmh, raw_speed=self.raw_speed,
                            in_range=self.in_range, values=[self.a0, self.a1, self.a2], last_update=self.last_update)

    @op("Verlauf ab Zeitpunkt since (Epoch-Sekunden)", result={'success': 'bool', 'history': 'list'})
    def get_history(self, since: float = 0):
        return self.success(history=[h for h in self.history if h['t'] > since], speed=self.speed,
                            in_range=self.in_range, error=self.error_msg)

    def range_text(self) -> str:
        if self.model is None or self.in_range is not False:
            return ''
        return f"außerhalb des kalibrierten Bereichs (bis {self.model.info.get('v_max', 0):.3f} m/s), Wert hochgerechnet"

    @category("ui")
    def part_index(self, p: base.Page, params={}):
        with p.section(h='Anemometer'):
            speed = '–' if self.speed is None else f'{self.speed:.2f}'
            p.div(f'<span style="font-size: 3em" id="wind_speed">{speed}</span> m/s '
                  f'<b id="wind_range" style="color: #d62728">{html.escape(self.range_text())}</b>')
            p.div(f'A{self.channel}-A{self.channel + 2}: {self.a0}, {self.a1}, {self.a2}, '
                  f'ADC: {html.escape(str(getattr(self.adc(), "type", "–")))}, Stand {self.last_update or "–"}')
            if self.error_msg:
                p.alert_error(html.escape(self.error_msg))
            if self.running:
                p(self.action_btn({'title': 'Stoppen', 'type': self.type, 'op': 'stop_measure'},
                                  hint="Stoppt die laufende Auswertung"))
            else:
                p(self.action_btn({'title': 'Starten', 'type': self.type, 'op': 'start_measure'},
                                  hint="Startet die laufende Auswertung"))
            p(self.action_btn({'title': 'Modell neu laden', 'type': self.type, 'op': 'reload_model'},
                              hint="Lädt model.json neu, z.B. nach einer neuen Kalibrierung"))
        with p.section(h=f'Verlauf (letzte {self.WINDOW} s)'):
            cfg = scatter_cfg([{'label': 'Geschwindigkeit (m/s)', 'line': True, 'data': self.history_points('v')},
                               {'label': 'ungeglättet', 'line': True, 'data': self.history_points('raw')}],
                              "Zeit (s)", "m/s")
            end = self.history[-1]['t'] - self.history_t0() if self.history else self.WINDOW
            cfg['options']['scales']['x'].update(min=round(end - self.WINDOW, 1), max=round(end, 1))
            add_chart(p, 'wind_history', cfg)
            p.js_ready(self.poll_js())
        self.part_model(p)
        p.pre('', id='result')
        super().part_index(p, params)

    """ Zeitfenster des Verlaufs-Diagramms in s, aeltere Werte verschwinden """
    WINDOW = 60

    def history_t0(self) -> float:
        """ Nullpunkt der Zeitachse: erster Wert im Fenster """
        if not self.history:
            return 0
        last = self.history[-1]['t']
        return next(h['t'] for h in self.history if h['t'] >= last - self.WINDOW)

    def history_points(self, key) -> list:
        if not self.history:
            return []
        t0, last = self.history_t0(), self.history[-1]['t']
        return [{'x': round(h['t'] - t0, 1), 'y': h[key]} for h in self.history if h['t'] >= last - self.WINDOW]

    def poll_js(self) -> str:
        job = json.dumps({'type': self.type, 'op': 'get_history'})
        last = self.history[-1]['t'] if self.history else 0
        return '''(function() {
    var since = ''' + str(last) + ''', t0 = ''' + str(self.history_t0()) + ''', win = ''' + str(self.WINDOW) + ''';
    function poll() {
        fetch(base_url(), post(Object.assign(''' + job + ''', {since: since}))).then(function(r) { return r.json(); })
        .then(function(d) {
            var ch = window.anemoCharts["wind_history"];
            d.history.forEach(function(h) {
                if (t0 === 0) { t0 = h.t; }
                ch.data.datasets[0].data.push({x: h.t - t0, y: h.v});
                ch.data.datasets[1].data.push({x: h.t - t0, y: h.raw});
                since = h.t;
            });
            if (since > 0) {
                var end = since - t0, start = end - win;
                ch.data.datasets.forEach(function(ds) { ds.data = ds.data.filter(function(pt) { return pt.x >= start; }); });
                ch.options.scales.x.min = start;
                ch.options.scales.x.max = end;
            }
            ch.update();
            if (d.speed !== null) { document.getElementById("wind_speed").textContent = d.speed.toFixed(2); }
            document.getElementById("wind_range").textContent = d.in_range === false ? ''' + json.dumps(
            f"außerhalb des kalibrierten Bereichs (bis {self.model.info.get('v_max', 0):.3f} m/s), Wert hochgerechnet"
            if self.model else '') + ''' : "";
            setTimeout(poll, 2000);
        }).catch(function() { setTimeout(poll, 5000); });
    }
    setTimeout(poll, 2000);
})();'''

    @category("ui")
    def part_model(self, p: base.Page):
        with p.section(h='Modell'):
            p.div(f'Datei: {html.escape(str(self.model_path))}')
            if self.model is None:
                p.div('Kein Modell geladen, erst mit AnemometerKalibrierung trainieren.')
                return
            info = self.model.info
            p.div(html.escape(self.model.describe()))
            part_channels(p, self.model)
            part_metrics(p, self.model.metrics)
