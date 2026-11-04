"""
Meshtastic-Knoten (verschoben aus nxbot.network)

    Config (runners):
        meshtastic: nxrunner.devices.meshtastic:MeshtasticBot(port="/dev/ttyACM0")
        meshtastic: nxrunner.devices.meshtastic:MeshtasticBot(host="192.168.178.50")
"""
from nwebclient import runner as r
from nwebclient import base
from nwebclient import web as w
from nwebclient import dev


class MeshtasticBot(r.BaseJobExecutor):
    """
    Verbindet sich per Serial oder TCP mit einem Meshtastic-Knoten, sendet Textnachrichten und meldet empfangene Pakete als Event.
    """

    MODULES = ['meshtastic']

    gpio = {1: 0x2, 2: 0x4, 3: 0x8, 4: 0x10, 5: 0x20, 6: 0x40, 7: 0x80, 8: 0x100, 9: 0x200, 10: 0x400, 11: 0x800,
            12: 0x1000, 13: 0x2000, 14: 0x4000, 15: 0x8000, 16: 0x10000, 17: 0x20000, 18: 0x40000, 19: 0x80000,
            20: 0x100000, 21: 0x200000, 22: 0x400000, 23: 0x800000, 24: 0x1000000, 25: 0x2000000, 26: 0x4000000,
            27: 0x8000000, 28: 0x10000000, 29: 0x20000000, 30: 0x40000000, 31: 0x80000000, 32: 0x100000000,
            33: 0x200000000, 34: 0x400000000, 35: 0x800000000, 36: 0x1000000000, 37: 0x2000000000, 38: 0x4000000000,
            39: 0x8000000000, 40: 0x10000000000, 41: 0x20000000000, 42: 0x40000000000, 43: 0x80000000000,
            44: 0x100000000000}

    def __init__(self, host=None, port=None):
        super().__init__('meshtastic')
        self.iface = None
        self.last = ''
        self.receive_count = 0
        self.define_vars('receive_count')
        self.define_event('on_message')
        self.define_sig(dev.PStr('message', 'Hello Node')).set_description('Sendet eine Textnachricht über Meshtastic').r(success=bool, message=str)
        if port is not None:
            self.connect_serial(port)
        if host is not None:
            self.connect_tcp(host)

    def sendText(self, message):
        self.iface.sendText(message)
        return self.success()

    def onReceive(self, packet, interface):  # pylint: disable=unused-argument
        """called when a packet arrives"""
        try:
            self.receive_count += 1
            self.last = str(packet)
            self.info(f"Received: {packet}")
            if packet['decoded'].get('portnum') == 'TEXT_MESSAGE_APP':
                message = packet['decoded']['payload'].decode('utf-8')
                fromnum = packet['fromId']
                self.last = message
                #shortname = next((node['user']['shortName'] for node in node_list if node['num'] == fromnum), 'Unknown')
                self.emit_event('on_message', message=f'{message}')
            else:
                self.emit_event('on_message', message=f'{packet}')
        except Exception as e:
            self.error(e)

    def connect_serial(self, port='/dev/ttyACM0'):
        from pubsub import pub
        import meshtastic.serial_interface as serial_interface
        self.iface = serial_interface.SerialInterface(port)
        pub.subscribe(self.onReceive, "meshtastic.receive")

    def connect_tcp(self, host):
        from pubsub import pub
        import meshtastic.tcp_interface as tcp
        self.iface = tcp.TCPInterface(hostname=host)
        pub.subscribe(self.onReceive, "meshtastic.receive")

    def execute(self, data):
        if 'message' in data:
            return self.sendText(data['message'])
        return super().execute(data)

    def part_gpios(self, p: base.Page, params={}):
        p("GPIOs")
        from meshtastic.remote_hardware import RemoteHardwareClient
        # meshtastic/remote_hardware.

    def part_index(self, p: base.Page, params={}):
        with p.section(h="Meshtastic"):
            p.prop("Receive Count", self.receive_count)
            p.form_input('message', "Message", id='message')
            p(self.action_btn_parametric("Send", dict(type=self.type, message='#message')))
            p(w.s(self.last))
            p(w.a("GPIO", self.link(self.part_gpios)))
        with p.section(h="Meshtastic"):
            p("Io25 ist die grüne blink led")
