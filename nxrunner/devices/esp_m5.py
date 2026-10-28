"""
M5StickS3 mit der Firmware esp_m5_multi (Wage, IR Sniffer, Tail RS485, Joystick2, Button, AS5600).

API des Geraets: esp_m5_multi/README.md (HTTP auf Port 80, immer genau ein Modul aktiv)

    GET  /api/status            System (Akku, WLAN, ...) und unter data die Werte des aktiven Moduls
    POST /api/action a=<aktion> Aktion des aktiven Moduls (text=... fuer RS485 send)
    POST /module id=<modul>     Modul wechseln (wird gespeichert, das Geraet startet neu)
    POST /restart               Neustart
    GET  /api/i2cscan           Adressen am Port A

    Config (runners):
        m5_multi: nxrunner.devices.esp_m5:EspM5()                        -> m5_multi.local (Standard-NXNAME)
        m5_multi: nxrunner.devices.esp_m5:EspM5(host="192.168.1.42")
        m5_multi: nxrunner.devices.esp_m5:EspM5(update_interval=2)      Variablen alle 2 s aktualisieren

    Variablen: module, battery, rpm, angle, revolutions, magnet (AS5600), weight_g (Wage), joy_x, joy_y,
    joy_button (Joystick2), btn_count (Button), ir_count (IR), last_update, last_error, update_interval.
    Werte inaktiver Module sind None.

    Jobs:
        {"type": "m5_multi", "op": "update"}                        Variablen einmalig aktualisieren
        {"type": "m5_multi", "op": "start_updates", "interval": 2}  periodische Aktualisierung starten
        {"type": "m5_multi", "op": "stop_updates"}                  periodische Aktualisierung stoppen
        {"type": "m5_multi", "op": "get_status"}                    /api/status
        {"type": "m5_multi", "op": "get_rotation"}                  AS5600: rpm, angle, revolutions, ...
        {"type": "m5_multi", "op": "get_weight"}                    Wage: weight in g
        {"type": "m5_multi", "op": "get_joystick"}                  Joystick2: x, y, button
        {"type": "m5_multi", "op": "get_log"}                       IR Sniffer / Tail RS485: empfangene Zeilen
        {"type": "m5_multi", "op": "run_action", "a": "reset"}      Aktion des aktiven Moduls
        {"type": "m5_multi", "op": "set_module", "id": "as5600"}    Modul wechseln
        {"type": "m5_multi", "op": "restart_device"}                Neustart
        {"type": "m5_multi", "op": "i2c_scan"}                      I2C-Scan am Port A
"""
import html
import time

from nwebclient import base
from nwebclient import runner as r
from nwebclient import web as w
from nwebclient import util as u
from nwebclient.runner import op


class EspM5(r.BaseJobExecutor):

    MODULES = ['requests']

    # Module der Firmware: id -> (Name, {Aktion: Beschriftung}), entspricht Module::id(), name() und actions()
    DEVICE_MODULES = {
        'scale': ("Wage", {'tare': "Tara", 'zero': "Kalibrierung 0 g"}),
        'ir': ("IR Sniffer", {'clear': "Log leeren"}),
        'rs485': ("Tail RS485", {'send': "Senden", 'clear': "Log leeren"}),
        'joy': ("Joystick2", {}),
        'btn': ("Button", {'trigger': "Auslösen"}),
        'as5600': ("AS5600", {'reset': "Umdrehungen zurücksetzen"}),
    }

    # Systemwerte aus /api/status: Schluessel -> (Titel, Formatierung)
    SYSTEM_VALUES = {
        'host': ("Hostname", str),
        'ip': ("IP", str),
        'ssid': ("WLAN", str),
        'rssi': ("Signal", lambda v: f"{v} dBm"),
        'battery': ("Akku", lambda v: f"{v} %" if v is not None and v >= 0 else '–'),
        'voltage': ("Spannung", lambda v: f"{v / 1000:.2f} V" if v else '–'),
        'charging': ("Lädt", lambda v: "ja" if v else "nein"),
        'uptime': ("Laufzeit", lambda v: f"{v // 3600} h {v % 3600 // 60} min"),
        'heap': ("Freier Heap", lambda v: u.format_size(v)),
    }

    # Werte der Module als Variablen des Runners: Modul -> {Schluessel in data: Variable}
    # (keine Namen mit get_<name>-Op, get_var wuerde sonst den Job ausfuehren)
    VALUE_VARS = {
        'as5600': {'rpm': 'rpm', 'angle': 'angle', 'revolutions': 'revolutions', 'magnet': 'magnet'},
        'scale': {'weight': 'weight_g'},
        'joy': {'x': 'joy_x', 'y': 'joy_y', 'button': 'joy_button'},
        'btn': {'count': 'btn_count'},
        'ir': {'count': 'ir_count'},
    }

    def __init__(self, type='m5_multi', host=None, timeout=3, update_interval=None):
        """
        :param host: IP oder Name des Geraets, ohne Angabe <type>.local (mDNS-Name = Name des Runners)
        :param timeout: Sekunden je HTTP-Anfrage
        :param update_interval: Sekunden zwischen den Aktualisierungen der Variablen, ohne Angabe aus
        """
        super().__init__(type)
        self.host = host
        self.timeout = float(timeout)
        self.updater = None
        self.update_interval = 5.0
        self.module = None
        self.battery = None
        self.last_update = None
        self.last_error = ''
        self.value_vars = [v for names in self.VALUE_VARS.values() for v in names.values()]
        for name in self.value_vars:
            setattr(self, name, None)
        self.define_vars('host', 'module', 'battery', *self.value_vars, 'last_update', 'last_error', 'update_interval')
        if update_interval is not None:
            self.start_updater(update_interval)

    def address(self) -> str:
        return self.host or f'{self.type}.local'

    # --- HTTP ---------------------------------------------------------------------------------

    def url(self, path):
        host = self.address().rstrip('/')
        if not host.startswith('http://') and not host.startswith('https://'):
            host = 'http://' + host
        return host + path

    def request(self, method, path, data=None):
        """ :return: JSON bzw. Text der Antwort """
        import requests
        res = requests.request(method, self.url(path), data=data, timeout=self.timeout)
        if res.status_code >= 400:
            raise RuntimeError(f'{method} {path}: HTTP {res.status_code} {res.text}')
        if res.headers.get('Content-Type', '').startswith('application/json'):
            return res.json()
        return res.text

    def device_status(self) -> dict:
        return self.request('GET', '/api/status')

    def module_data(self, module_id) -> dict:
        """ Werte des aktiven Moduls, Fehler wenn ein anderes Modul aktiv ist """
        status = self.device_status()
        if status.get('module') != module_id:
            raise RuntimeError(f"Modul {self.module_name(module_id)} nicht aktiv (aktiv: {self.module_name(status.get('module'))})")
        return status.get('data', {})

    def module_name(self, module_id) -> str:
        return self.DEVICE_MODULES.get(module_id, (str(module_id), {}))[0]

    # --- Variablen ----------------------------------------------------------------------------

    def set_value(self, name, value):
        if getattr(self, name, None) != value:
            self.emit_var_change(name, value)
            setattr(self, name, value)

    def apply_status(self, status: dict):
        """ Variablen aus /api/status setzen, Werte inaktiver Module werden None """
        module = status.get('module')
        data = status.get('data', {})
        self.set_value('module', module)
        self.set_value('battery', status.get('battery'))
        for mid, names in self.VALUE_VARS.items():
            for key, name in names.items():
                self.set_value(name, data.get(key) if mid == module else None)
        self.last_update = time.strftime('%Y-%m-%d %H:%M:%S')
        self.set_value('last_error', '')

    def update_values(self):
        try:
            self.apply_status(self.device_status())
        except Exception as e:
            self.set_value('last_error', f'{type(e).__name__}: {e}')

    def start_updater(self, interval=None):
        if interval is not None:
            self.set_update_interval(interval)
        if self.updater is None:
            self.updater = self.periodic(self.update_interval, self.update_values)

    def stop_updater(self):
        if self.updater is not None:
            self.updater.stop()
            self.updater = None

    def set_update_interval(self, value):
        """ auch fuer setvar, ein laufender Thread uebernimmt das Intervall """
        self.emit_var_change('update_interval', value)
        self.update_interval = max(0.5, float(value))
        if self.updater is not None:
            self.updater.time_s = self.update_interval

    def call(self, fn, message='ok'):
        """ Job-Ergebnis: success mit value = fn(), bei Fehlern fail """
        try:
            return self.success(message, value=fn())
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')

    # --- Jobs ---------------------------------------------------------------------------------

    @op("Setzt die Adresse des Geraets (ip oder host)", result={'success': 'bool', 'message': 'str'})
    def connect(self, ip: str = None, host: str = None):
        ip = str(ip or host or '').strip()
        if ip == '':
            return self.fail('ip fehlt')
        self.host = ip
        return self.success(f'Adresse {ip} gesetzt', ui_reload=True)

    @op("Liefert /api/status: System und unter data die Werte des aktiven Moduls",
        result={'success': 'bool', 'message': 'str', 'value': 'dict'})
    def get_status(self):
        def status():
            res = self.device_status()
            self.apply_status(res)
            return res
        return self.call(status)

    @op("Aktualisiert die Variablen (rpm, weight_g, joy_x, ...) einmalig", result={'success': 'bool', 'message': 'str'})
    def update(self):
        self.update_values()
        if self.last_error:
            return self.fail(self.last_error)
        return self.success(f'Modul {self.module_name(self.module)}', ui_reload=True)

    @op("Startet die periodische Aktualisierung der Variablen (interval in s)",
        result={'success': 'bool', 'message': 'str'})
    def start_updates(self, interval: float = None):
        self.start_updater(interval)
        return self.success(f'Aktualisierung alle {self.update_interval:g} s', ui_reload=True)

    @op("Stoppt die periodische Aktualisierung der Variablen", result={'success': 'bool', 'message': 'str'})
    def stop_updates(self):
        self.stop_updater()
        return self.success('Aktualisierung gestoppt', ui_reload=True)

    @op("AS5600: Drehzahl (rpm), Winkel (angle in Grad), Umdrehungen (revolutions) und Magnetstatus",
        result={'success': 'bool', 'message': 'str', 'value': 'dict'})
    def get_rotation(self):
        return self.call(lambda: self.module_data('as5600'))

    @op("Wage: Gewicht (weight in g)", result={'success': 'bool', 'message': 'str', 'value': 'dict'})
    def get_weight(self):
        return self.call(lambda: self.module_data('scale'))

    @op("Joystick2: x, y und button", result={'success': 'bool', 'message': 'str', 'value': 'dict'})
    def get_joystick(self):
        return self.call(lambda: self.module_data('joy'))

    @op("IR Sniffer / Tail RS485: empfangene Zeilen, neueste zuerst",
        result={'success': 'bool', 'message': 'str', 'value': 'list'})
    def get_log(self):
        def log():
            status = self.device_status()
            if 'log' not in status.get('data', {}):
                raise RuntimeError(f"Modul {self.module_name(status.get('module'))} hat kein Log")
            return status['data']['log']
        return self.call(log)

    @op("Fuehrt eine Aktion des aktiven Moduls aus (a), text fuer RS485 send",
        result={'success': 'bool', 'message': 'str'})
    def run_action(self, a: str = '', text: str = None):
        data = {'a': a} if text is None else {'a': a, 'text': text}
        try:
            res = self.request('POST', '/api/action', data)
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')
        if res == 'unknown action':
            return self.fail(f'Unbekannte Aktion {a} für das aktive Modul')
        return self.success(str(res))

    @op("Wechselt das Modul (id: scale, ir, rs485, joy, btn, as5600), das Geraet startet danach neu",
        result={'success': 'bool', 'message': 'str'})
    def set_module(self, id: str = ''):
        if id not in self.DEVICE_MODULES:
            return self.fail(f'Unbekanntes Modul {id}')
        try:
            self.request('POST', '/module', {'id': id})
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')
        return self.success(f'Modul {self.module_name(id)} gewählt, Gerät startet neu')

    @op("Startet das Geraet neu", result={'success': 'bool', 'message': 'str'})
    def restart_device(self):
        try:
            self.request('POST', '/restart')
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')
        return self.success('Neustart')

    @op("I2C-Scan am Port A", result={'success': 'bool', 'message': 'str'})
    def i2c_scan(self):
        try:
            return self.success(self.request('GET', '/api/i2cscan'))
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')

    # --- Seite --------------------------------------------------------------------------------

    def part_index(self, p: base.Page, params={}):
        with p.section(h='M5 Multi'):
            p.div('Gerät: ' + w.a(html.escape(self.address()), html.escape(self.url('/'))) +
                  ('' if self.host else ' <small>(aus dem Namen des Runners)</small>'))
            p.form_input('ip', 'Adresse', id='m5_ip', value=html.escape(str(self.host or ''), quote=True),
                         placeholder=html.escape(f'{self.address()} oder 192.168.1.42', quote=True))
            p(self.action_btn_parametric('Adresse setzen', {'type': self.type, 'op': 'connect', 'ip': '#m5_ip'}))
        try:
            status = self.device_status()
        except Exception as e:
            p.alert_error(f'Gerät nicht erreichbar: {html.escape(type(e).__name__)}: {html.escape(str(e))}')
        else:
            self.apply_status(status)
            self.part_module(p, status, params)
            self.part_system(p, status, params)
        self.part_updater(p, params)
        p.pre('', id='result')
        super().part_index(p, params)

    def part_module(self, p: base.Page, status: dict, params={}):
        """ Werte und Aktionen des aktiven Moduls, Modulwahl """
        module = status.get('module')
        name, actions = self.DEVICE_MODULES.get(module, (str(module), {}))
        data = status.get('data', {})
        with p.section(h='Modul: ' + html.escape(name)):
            values = [{'key': k, 'value': v} for k, v in data.items() if k != 'log']
            p.grid(values, [w.Grid.col('key', "Wert"), w.Grid.col('value', "")], empty="Keine Werte")
            if 'log' in data:
                p.pre(html.escape('\n'.join(map(str, data['log']))) or '(leer)')
            if 'send' in actions:
                p.form_input('text', 'Text', id='m5_text')
            for a, title in actions.items():
                job = {'type': self.type, 'op': 'run_action', 'a': a}
                if a == 'send':
                    p(self.action_btn_parametric(title, {**job, 'text': '#m5_text'}))
                else:
                    p(self.action_btn({'title': title, **job}))
            p.div('Modul wechseln (Gerät startet neu): ' + ' '.join(
                self.action_btn({'title': title, 'type': self.type, 'op': 'set_module', 'id': mid})
                for mid, (title, _) in self.DEVICE_MODULES.items() if mid != module))

    def part_updater(self, p: base.Page, params={}):
        with p.section(h='Aktualisierung der Variablen'):
            if self.updater is None:
                p.div('Aus')
                p.form_input('interval', 'Intervall (s)', id='m5_interval', value=f'{self.update_interval:g}')
                p(self.action_btn_parametric('Starten', {'type': self.type, 'op': 'start_updates',
                                                         'interval': '#m5_interval'}))
            else:
                p.div(f'Aktiv, alle {self.update_interval:g} s')
                p(self.action_btn({'title': 'Stoppen', 'type': self.type, 'op': 'stop_updates'}))
            p.div(f'Letzte Aktualisierung: {html.escape(str(self.last_update or "–"))}')
            if self.last_error:
                p.alert_error(html.escape(self.last_error))

    def part_system(self, p: base.Page, status: dict, params={}):
        with p.section(h='System'):
            rows = [{'title': title, 'value': fmt(status[k])} for k, (title, fmt) in self.SYSTEM_VALUES.items() if k in status]
            p.grid(rows, [w.Grid.col('title', "Wert"), w.Grid.col('value', "")])
            p(self.action_btn({'title': 'I2C-Scan', 'type': self.type, 'op': 'i2c_scan'}))
            p(self.action_btn({'title': 'Neustart', 'type': self.type, 'op': 'restart_device'}))
