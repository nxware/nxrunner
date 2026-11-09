"""
Dashboard fuer ein E-Paper-Display (CrowPanel 2.13", ESP32-S3, 250x122) mit der Firmware esp_epaper.

Das Geraet schlaeft die meiste Zeit (Deep Sleep) und ist daher nicht erreichbar. Nach jedem Aufwachen:
    1. UDP-Broadcast an Port 7070: "nxepaper <NX_NAME> <ip>"
    2. GET NPY_URL?type=<NX_NAME>&r=1&v=0      -> Antwort ist ein nxmc-Script (ein Befehl pro Zeile, CMD.md)
    3. Anzeige aktualisieren
    4. GET NPY_URL?type=<NX_NAME>&w=done
    5. Deep Sleep (Dauer aus dem Script, sonst SLEEP_SECONDS der Firmware)
Der Runner muss daher so heissen wie NX_NAME in config.h der Firmware.

Angezeigt werden die ersten MAX_ROWS Eintraege des Dashboards als Zeilen "Label  Wert"
(Firmware-Items t1..t6 fuer Labels, v1..v6 fuer Werte).

    Config (runners):
        crowpanel: nxrunner.devices.epaper:EspEPaperDashboard(items=["pin1.state"])
        crowpanel: nxrunner.devices.epaper:EspEPaperDashboard(db=True, sleep=600)
        crowpanel: nxrunner.devices.epaper:EspEPaperDashboard(udp=False)     ohne UdpServer

    Gibt es keinen UdpServer auf udp_port, wird einer angelegt (udp7070), um die Broadcasts zu empfangen.

    Jobs:
        {"type": "crowpanel", "r": 1}                     Script fuer das Geraet (Text)
        {"type": "crowpanel", "w": "done"}                Geraet hat die Anzeige aktualisiert
        {"type": "crowpanel", "op": "get_script"}         Script als value
        {"type": "crowpanel", "op": "set_sleep", "seconds": 600}
"""
import html
import time

from nwebclient import base
from nwebclient import util as u
from nwebclient.runner import op
from nwebclient.runner.dashboard import Dashboard
from nwebclient.runner.dispatcher import LazyDispatcher

from nxrunner.devices.cyd import DashboardCYD


class EspEPaperDashboard(Dashboard):

    ANNOUNCE_PREFIX = 'nxepaper'
    MAX_ROWS = 6        # Items t1..t6 / v1..v6 der Firmware
    WIDTH = 250         # Pixel
    CHAR_WIDTH = 8      # Schriftgroesse 16 -> 8 Pixel je Zeichen
    MARGIN = 5          # x der Labels in der Firmware
    MAX_LABEL = 14
    MIN_SLEEP = 60

    def __init__(self, type='epaper', items=None, file=None, interval=5000, db: bool = False,
                 sleep=None, udp=True, udp_port=7070):
        """
        :param sleep: Sekunden Deep Sleep bis zur naechsten Aktualisierung, ohne Angabe SLEEP_SECONDS der Firmware
        :param udp: bei Bedarf einen UdpServer auf udp_port anlegen und die Broadcasts des Geraets auswerten
        """
        super().__init__(type, items=items, file=file, interval=interval, db=db)
        self.sleep = None
        if sleep is not None:
            self.set_sleep_seconds(sleep)
        self.udp = udp
        self.udp_port = int(udp_port)
        self.udp_listening = False
        self.ip = None
        self.last_announce = None  # Zeitpunkt des letzten Broadcasts
        self.last_request = None   # Zeitpunkt des letzten Script-Abrufs
        self.last_done = None      # Zeitpunkt der letzten Rueckmeldung w=done
        self.last_error = None
        self.define_vars('ip', 'sleep', 'last_announce', 'last_request', 'last_done', 'last_error')

    def onOwnerChanged(self, newOnwer):
        super().onOwnerChanged(newOnwer)
        if self.udp and not self.udp_listening:
            self.listen_udp()

    # --- UDP ----------------------------------------------------------------------------------

    def udp_server(self, dispatcher: LazyDispatcher):
        """ vorhandener UdpServer auf udp_port, sonst ein neuer (udp<port>) """
        for runner in dispatcher.get_runner_by_class('UdpServer'):
            if int(runner.port) == self.udp_port:
                return runner
        name = f'udp{self.udp_port}'
        if name in dispatcher:
            runner = dispatcher.get_runner(name)
            if runner is not None and int(getattr(runner, 'port', -1)) == self.udp_port:
                return runner
        from nxrunner.network.udp import UdpServer
        runner = UdpServer(port=self.udp_port)
        dispatcher.add_runner(runner)
        self.info(f'UdpServer {runner.type} angelegt')
        return runner

    def listen_udp(self):
        dispatcher = self.getParentClass(LazyDispatcher)
        if dispatcher is None:
            return
        try:
            self.udp_server(dispatcher).add_event_listener('on_udp_message', self.on_udp_message)
            self.udp_listening = True
        except Exception as e:
            self.last_error = f'UDP {self.udp_port}: {type(e).__name__}: {e}'
            self.error(self.last_error)

    def on_udp_message(self, data):
        """ "nxepaper <name> <ip>" des eigenen Geraets (name = type des Runners) """
        parts = str(data.get('message', '')).split()
        if len(parts) == 3 and parts[0] == self.ANNOUNCE_PREFIX and parts[1].lower() == self.type.lower():
            self.emit_var_change('ip', parts[2])
            self.ip = parts[2]
            self.last_announce = time.time()

    # --- Script -------------------------------------------------------------------------------

    @staticmethod
    def quote(text) -> str:
        """ Argument nach CMD.md: "..." mit \\" und \\\\ """
        s = DashboardCYD.ascii(text).replace('°', '').replace('\\', '\\\\').replace('"', '\\"')
        return f'"{s}"'

    @staticmethod
    def value_text(value) -> str:
        if value is None:
            return '-'
        if isinstance(value, bool):
            return 'ja' if value else 'nein'
        if isinstance(value, float):
            return f'{value:.2f}'.rstrip('0').rstrip('.')
        return str(value)

    def rows(self) -> list:
        """ (Label, Wert) der ersten MAX_ROWS Eintraege """
        return [(self.label(item)[:self.MAX_LABEL], self.value_text(item['value']))
                for item in self.values()[:self.MAX_ROWS]]

    def script(self) -> str:
        """ nxmc-Script fuer das Geraet: Texte und x-Position der Werte, leere Zeilen loeschen """
        rows = self.rows()
        label_len = max([len(label) for label, _ in rows] + [1])
        value_x = self.MARGIN + (label_len + 1) * self.CHAR_WIDTH
        value_len = max(1, (self.WIDTH - value_x) // self.CHAR_WIDTH)
        lines = []
        for i in range(self.MAX_ROWS):
            label, value = rows[i] if i < len(rows) else ('', '')
            lines.append(f't{i + 1} set-text {self.quote(label)}')
            lines.append(f'v{i + 1} set-x {value_x}')
            lines.append(f'v{i + 1} set-text {self.quote(value[:value_len])}')
        if self.sleep is not None:
            lines.append(f'epaper sleep {self.sleep}')
        return '\n'.join(lines) + '\n'

    def set_sleep_seconds(self, value):
        self.emit_var_change('sleep', value)
        self.sleep = None if value in (None, '') else max(self.MIN_SLEEP, int(float(value)))

    # --- Jobs ---------------------------------------------------------------------------------

    def execute(self, data):
        # Abrufe der Firmware ohne op: ?type=<name>&r=1&v=0 und ?type=<name>&w=done
        if 'op' not in data and 'r' in data:
            return self.device_request()
        if 'op' not in data and 'w' in data:
            self.last_done = time.time()
            return self.success('ok')
        return super().execute(data)

    def device_request(self) -> str:
        """ Antwort als Text, der Runner-Server sendet Strings unveraendert """
        self.last_request = time.time()
        try:
            script = self.script()
            self.last_error = None
            return script
        except Exception as e:
            self.last_error = f'{type(e).__name__}: {e}'
            self.error(self.last_error)
            return ''

    @op("Liefert das Script, das das E-Paper beim naechsten Aufwachen erhaelt",
        result={'success': 'bool', 'message': 'str', 'value': 'str'})
    def get_script(self):
        return self.success('ok', value=self.script())

    @op("Setzt den Deep Sleep des Geraets in Sekunden (leer = Standard der Firmware), gilt ab dem naechsten Abruf",
        result={'success': 'bool', 'message': 'str'})
    def set_sleep(self, seconds: str = ''):
        try:
            self.set_sleep_seconds(seconds)
        except ValueError:
            return self.fail(f'Ungueltige Sekunden: {seconds}')
        return self.success(f'Deep Sleep: {self.sleep or "Standard der Firmware"} s', ui_reload=True)

    # --- Seite --------------------------------------------------------------------------------

    def part_index(self, p: base.Page, params={}):
        with p.section(h='E-Paper'):
            self.part_device(p, params)
            p.form_input('seconds', 'Deep Sleep (s)', id='epaper_sleep',
                         value=html.escape(str(self.sleep or ''), quote=True), placeholder='Standard der Firmware')
            p(self.action_btn_parametric('Setzen', {'type': self.type, 'op': 'set_sleep', 'seconds': '#epaper_sleep'},
                                         hint='Schlafdauer zwischen zwei Aktualisierungen, wirkt beim nächsten Abruf'))
            p(self.action_btn({'title': 'Script anzeigen', 'type': self.type, 'op': 'get_script'},
                              hint='Befehle, die das Gerät beim nächsten Aufwachen erhält'))
        super().part_index(p, params)

    def part_device(self, p: base.Page, params={}):
        p.div(f'Gerätename (NX_NAME): {html.escape(self.type)}')
        p.div('IP: ' + html.escape(str(self.ip or '–')))
        for title, t in [('Letzter Broadcast', self.last_announce), ('Letzter Abruf', self.last_request),
                         ('Letzte Aktualisierung', self.last_done)]:
            if t:
                p.div(f'{title}: {u.format_age(t)}', title=u.format_datetime(t))
        if not self.udp:
            p.div('UDP aus')
        elif not self.udp_listening:
            p.div(f'UDP {self.udp_port}: nicht verbunden')
        if self.last_error:
            p.alert_error(html.escape(self.last_error))
