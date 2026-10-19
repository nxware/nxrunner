"""
Dashboard, das seine Werte zusaetzlich an ein CYD (Cheap Yellow Display, ESP32 + LVGL) sendet.

API des CYD: esp_display/docs/HTTP.md (REST auf Port 80, Kennzahlen als Kacheln)

    Config (runners):
        dashboard_cyd: nxrunner.devices:DashboardCYD(items=["pin1.state"])         -> sendet an dashboard_cyd.local
        dashboard_cyd: nxrunner.devices:DashboardCYD(host="192.168.1.42", items=["pin1.state"])
        dashboard_cyd: nxrunner.devices:DashboardCYD(host="dashboard_cyd.local", db=True, title="Labor",
                           metrics={"pin1.state": {"unit": "°C", "min": 0, "max": 40, "color": "#4CAF50"}})

    Alle Eintraege des Dashboards werden mit einem PUT /api/metrics als Kacheln gesendet (gleiche Reihenfolge,
    im Dashboard entfernte Eintraege verschwinden auch vom Display). Gesendet wird alle push_interval Sekunden,
    wenn sich etwas geaendert hat, sonst spaetestens alle keepalive Sekunden.

    Das CYD meldet sich nach dem WLAN-Connect per UDP-Broadcast {"type": "dashboard_cyd", "op": "connect", "ip": ...}.
    Heisst der Runner wie dieser type (Standard dashboard_cyd) und leitet ein UDP-Empfaenger die Broadcasts an den
    Dispatcher weiter, wird die IP automatisch uebernommen (op=connect).

    Jobs:
        {"type": "dashboard_cyd", "op": "push"}                   jetzt senden
        {"type": "dashboard_cyd", "op": "connect", "ip": "..."}   Adresse setzen und senden
        {"type": "dashboard_cyd", "op": "info"}                   /api/info des Geraets
"""
import html
import json
import re
import threading
import time
import unicodedata

from nwebclient import base
from nwebclient import web as w
from nwebclient import util as u
from nwebclient.base import PeriodedThread
from nwebclient.runner.dashboard import Dashboard


class DashboardCYD(Dashboard):

    MODULES = ['requests']

    # Optionen einer Kachel, die per metrics je Eintrag gesetzt werden koennen (siehe HTTP.md, Kennzahl)
    METRIC_OPTIONS = ['label', 'unit', 'color', 'min', 'max', 'decimals', 'wide']
    MAX_METRICS = 24
    MAX_TEXT = 40

    def __init__(self, type='dashboard_cyd', host=None, items=None, file=None, interval=5000, db: bool = False,
                 push_interval=10, keepalive=60, timeout=3, title=None, metrics=None):
        """
        :param host: IP oder Name des CYD, ohne Angabe <type>.local (mDNS-Name des CYD = Name des Runners),
                     kann per op=connect gesetzt werden
        :param push_interval: Sekunden zwischen zwei Pruefungen/Sendungen
        :param keepalive: spaetestens nach so vielen Sekunden wird auch ohne Aenderung gesendet
        :param title: Titel in der Kopfzeile des Displays (optional)
        :param metrics: {Label: {unit, min, max, color, decimals, wide, label}} Optionen je Eintrag
        """
        super().__init__(type, items=items, file=file, interval=interval, db=db)
        self.host = host
        self.push_interval = float(push_interval)
        self.keepalive = float(keepalive)
        self.timeout = float(timeout)
        self.title = title
        self.metric_options = metrics or {}
        self.last_payload = None
        self.last_push = None  # Zeitpunkt der letzten erfolgreichen Sendung
        self.last_error = None
        self.title_sent = None
        self.push_lock = threading.Lock()
        self.thread = None
        self.define_vars('host', 'last_push', 'last_error')

    # --- Thread -------------------------------------------------------------------------------

    def onOwnerChanged(self, newOnwer):
        super().onOwnerChanged(newOnwer)
        # erst senden, wenn der Runner im Dispatcher haengt, sonst liefern die Eintraege nur Fehler
        if self.thread is None:
            self.thread = PeriodedThread(self.tick, self.push_interval, start=False)
            self.thread.daemon = True
            self.thread.start()

    def tick(self):
        self.push(force=False)

    def address(self) -> str:
        """ gesetzte Adresse oder <type>.local, erst hier ermittelt, da der Dispatcher den type noch setzen kann """
        return str(self.host).strip() if self.host else f'{self.type}.local'

    # --- Kennzahlen ---------------------------------------------------------------------------

    @staticmethod
    def ascii(text) -> str:
        """ Display-Schriften enthalten nur ASCII und °, Umlaute umschreiben, Rest entfernen """
        s = str(text)
        for a, b in {'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue', 'ß': 'ss'}.items():
            s = s.replace(a, b)
        s = unicodedata.normalize('NFKD', s)
        return ''.join(c for c in s if c == '°' or (ord(c) < 128 and not unicodedata.combining(c)))

    @staticmethod
    def metric_id(label, used) -> str:
        """ ID nach HTTP.md: 1-32 Zeichen A-Z a-z 0-9 _ -, eindeutig innerhalb der Sendung """
        base_id = re.sub(r'[^A-Za-z0-9_-]', '_', DashboardCYD.ascii(label)).strip('_')[:32] or 'item'
        mid, n = base_id, 2
        while mid in used:
            suffix = f'_{n}'
            mid = base_id[:32 - len(suffix)] + suffix
            n += 1
        used.add(mid)
        return mid

    def metric_value(self, value):
        """ Zahl oder Text (bzw. None), alles andere als Text """
        if value is None or (isinstance(value, (int, float)) and not isinstance(value, bool)):
            return value
        if isinstance(value, bool):
            return 'ja' if value else 'nein'
        text = value if isinstance(value, str) else json.dumps(value)
        return self.ascii(text)[:self.MAX_TEXT]

    def metrics(self) -> list:
        """ Kennzahlen fuer PUT /api/metrics aus den aktuellen Werten des Dashboards """
        used = set()
        res = []
        for item in self.values()[:self.MAX_METRICS]:
            label = self.label(item)
            options = {k: v for k, v in self.metric_options.get(label, {}).items() if k in self.METRIC_OPTIONS}
            metric = {'id': self.metric_id(label, used), 'label': label, **options,
                      'value': self.metric_value(item['value'])}
            metric['label'] = self.ascii(metric['label'])
            if 'unit' in metric:
                metric['unit'] = self.ascii(metric['unit'])
            res.append(metric)
        return res

    # --- HTTP ---------------------------------------------------------------------------------

    def url(self, path):
        host = self.address().rstrip('/')
        if not host.startswith('http://') and not host.startswith('https://'):
            host = 'http://' + host
        return host + path

    def request(self, method, path, body=None):
        import requests
        r = requests.request(method, self.url(path), json=body, timeout=self.timeout)
        if r.status_code >= 400:
            try:
                message = r.json().get('error', r.text)
            except ValueError:
                message = r.text
            raise RuntimeError(f'{method} {path}: HTTP {r.status_code} {message}')
        return r.json() if r.content else None

    def push(self, force=True):
        """ Sendet alle Kennzahlen (und ggf. den Titel), ohne force nur bei Aenderung oder nach keepalive """
        with self.push_lock:
            payload = self.metrics()
            due = self.last_push is None or time.time() - self.last_push >= self.keepalive
            if not force and payload == self.last_payload and not due:
                return False
            try:
                if self.title and self.title_sent != self.title:
                    self.request('PUT', '/api/dashboard', {'title': self.ascii(self.title)})
                    self.title_sent = self.title
                self.request('PUT', '/api/metrics', payload)
            except Exception as e:
                self.last_error = f'{type(e).__name__}: {e}'
                self.title_sent = None  # nach einem Neustart des Geraets erneut senden
                raise
            self.last_payload = payload
            self.last_push = time.time()
            self.last_error = None
            return True

    # --- Jobs ---------------------------------------------------------------------------------

    def execute_push(self, data):
        try:
            self.push(force=True)
        except Exception as e:
            return self.fail(self.last_error or str(e))
        return self.success(f'{len(self.last_payload)} Kennzahlen an {self.address()} gesendet')

    def execute_connect(self, data):
        """ Broadcast des CYD nach dem WLAN-Connect bzw. Adresse von Hand setzen """
        ip = str(data.get('ip') or data.get('host') or '').strip()
        if ip == '':
            return self.fail('ip fehlt')
        self.host = ip
        self.title_sent = None
        try:
            self.push(force=True)
        except Exception as e:
            return self.fail(f'{ip} gesetzt, Senden fehlgeschlagen: {self.last_error or e}')
        return self.success(f'{ip} gesetzt, {len(self.last_payload)} Kennzahlen gesendet', ui_reload=True)

    def execute_info(self, data):
        try:
            return self.success('ok', value=self.request('GET', '/api/info'))
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')

    # --- Seite --------------------------------------------------------------------------------

    def part_index(self, p: base.Page, params={}):
        with p.section(h='CYD'):
            p.div('Gerät: ' + html.escape(self.address()) + ('' if self.host else ' <small>(aus dem Namen des Runners)</small>'))
            if self.last_push:
                p.div(f'Zuletzt gesendet: {u.format_age(self.last_push)}', title=u.format_datetime(self.last_push))
            if self.last_error:
                p.div('Fehler: ' + html.escape(self.last_error))
            p.form_input('ip', 'Adresse', id='cyd_ip', value=html.escape(str(self.host or ''), quote=True),
                         placeholder=html.escape(f'{self.address()} oder 192.168.1.42', quote=True))
            p(self.action_btn_parametric('Adresse setzen', {'type': self.type, 'op': 'connect', 'ip': '#cyd_ip'}))
            p(self.action_btn({'title': 'Jetzt senden', 'type': self.type, 'op': 'push'}))
            p(self.action_btn({'title': 'Geräteinfo', 'type': self.type, 'op': 'info'}))
        super().part_index(p, params)
