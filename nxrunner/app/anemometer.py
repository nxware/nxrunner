"""
Kalibrierung eines Heizdraht-Anemometers.

Benoetigte Runner:
    m5        EspM5 (nxrunner.devices.esp_m5) mit AS5600, misst die Drehzahl der Scheibe
    esphome   EspHomeDevice
    ads       ADC mit 3 analogen Signalen (Kanal channel bis channel+2), erkannt an der Signatur
              {"read_a": 1, "channel": n} -> {"value": ...}, z.B. nxbot.i2c.ADS oder ein EspHomeDevice
              mit den Entities analog_a0, analog_a1 und analog_a2

    Config (runners):
        anemometer: nxrunner.app.anemometer:AnemometerKalibrierung()
        anemometer: nxrunner.app.anemometer:AnemometerKalibrierung(m5="m5_multi", esphome="esphome", ads="ads", channel=0)
        anemometer: nxrunner.app.anemometer:AnemometerKalibrierung(ads="esp-anemometer")   EspHomeDevice als ADC

    Ohne Angabe wird der erste passende (bereits geladene) Runner verwendet, auf der Seite kann ein anderer gewaehlt
    werden. Sind alle drei vorhanden, fuehrt der Button Kalibrierung auf page=calibration.

    Jobs:
        {"type": "anemometer", "op": "set_runners", "m5": "m5_multi", "esphome": "esphome", "ads": "ads"}
"""
import html
import json
import math

from nwebclient import base
from nwebclient import runner as r
from nwebclient import web as w
from nwebclient.runner import op

from nxrunner.devices.esp_m5 import EspM5


class AnemometerKalibrierung(r.BaseJobExecutor):

    TAGS = ['app', 'anemometer']

    # Rolle -> (Titel, Test ob ein Runner passt)
    ROLES = {
        'm5': ("EspM5 (Drehzahl der Scheibe)", lambda x: isinstance(x, EspM5)),
        'esphome': ("EspHomeDevice", lambda x: any(c.__name__ == 'EspHomeDevice' for c in type(x).__mro__)),
        'ads': ("ADC, 3 analoge Signale (read_a)",
                lambda x: any('read_a' in s and 'channel' in s for s in getattr(x, 'signatures', []))),
    }

    """ Anzahl der analogen Signale des Anemometers """
    CHANNELS = 3

    """ Abstand des Sensors zur Drehachse (Radius) in cm """
    distance = 15

    def __init__(self, type='anemometer', m5=None, esphome=None, ads=None, channel=0):
        """
        :param m5: Name des EspM5-Runners, ohne Angabe der erste passende
        :param esphome: Name des EspHomeDevice-Runners, ohne Angabe der erste passende
        :param ads: Name des Runners mit der Signatur read_a, ohne Angabe der erste passende
        :param channel: erster der 3 Kanaele des ADC fuer das Anemometer
        """
        super().__init__(type)
        self.m5 = m5
        self.esphome = esphome
        self.ads = ads
        self.channel = int(channel)
        self.define_vars('distance')

    # --- Runner -------------------------------------------------------------------------------

    def executor(self):
        return self.getRoot().jobexecutor

    def runner_by_name(self, name):
        get_runner = getattr(self.executor(), 'get_runner', None)
        return get_runner(name) if name and get_runner is not None else None

    def candidates(self, role) -> dict:
        """ :return: {Name: Runner} der geladenen Runner, die fuer die Rolle passen """
        test = self.ROLES[role][1]
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
        """ Liest einen Kanal ueber den Job read_a (Signatur), damit funktionieren ADS und EspHomeDevice gleich """
        res = ads.execute({'type': ads.type, 'read_a': 1, 'channel': channel})
        if not res.get('success', False):
            raise ValueError(res.get('message', 'read_a fehlgeschlagen'))
        return res['value']

    def get_speed(self, rpm):
        """ Geschwindigkeit in m/s des Sensors auf der Scheibe, Umfang bei distance (cm) mal Umdrehungen je Sekunde """
        return 2 * math.pi * float(self.distance) / 100 * float(rpm) / 60

    # --- Seite --------------------------------------------------------------------------------

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
            p(self.action_btn_parametric('Übernehmen', job, on_success='function(r) { location.reload(); }'))
        missing = self.missing()
        if missing:
            p('<button disabled>Kalibrierung</button>')
            p.div('Fehlt: ' + html.escape(', '.join(missing)))
        else:
            p(w.button_js('Kalibrierung', 'location.href = ' + json.dumps(self.link(self.part_calibration)) + ';'))
        p.pre('', id='result')
        super().part_index(p, params)

    def part_calibration(self, p: base.Page, params={}):
        missing = self.missing()
        if missing:
            p.alert_error('Fehlt: ' + html.escape(', '.join(missing)))
            p(w.a('Zurück', self.link(self.part_index)))
            return
        m5, esphome, ads = self.runner('m5'), self.runner('esphome'), self.runner('ads')
        with p.section(h='Kalibrierung'):
            p.div(f'EspM5: {html.escape(m5.type)}, EspHomeDevice: {html.escape(esphome.type)}, '
                  f'ADC: {html.escape(ads.type)} Kanal {self.channel}-{self.channel + self.CHANNELS - 1}')
            p.div(f'Drehzahl: {html.escape(str(m5.rpm))} rpm')
            if m5.rpm is not None:
                p.div(f'Geschwindigkeit: {self.get_speed(m5.rpm):.2f} m/s (Abstand {self.distance:g} cm)')
            for channel in self.channels():
                try:
                    p.div(f'A{channel}: {html.escape(str(self.read_analog(ads, channel)))}')
                except Exception as e:
                    p.alert_error(f'A{channel}: {html.escape(type(e).__name__)}: {html.escape(str(e))}')
            p(w.a('Zurück', self.link(self.part_index)))
            # TODO Start Button -> RPM Langsam erhöhen RPM Channel 0..2 in einem Graph darstellen und speichern
            # TODO einstellung für Start um werte langsam zu steigern
            # TODO auswertung aus channel 0..2 soll abgeleitet werden wie schnell sich das Messgerät bewegt bzw. die Wind geschwindigkeit
            # TODO fehlerquote berechnen
