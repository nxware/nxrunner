
import time
import socket
import json

from nwebclient.runner import BaseJobExecutor
from nwebclient import nx
from nwebclient import util as u
from nwebclient import base as b
from nwebclient import web as w


class UdpServer(BaseJobExecutor):
    """
      nxrunner.network.udp:UdpServer(port=7070)
    """

    def __init__(self, host='0.0.0.0', port=7070, store=False, start=True, autosave=False, chunk_size=100000, process_npy = False, args: u.Args = {}):
        super().__init__('udp' + str(port))
        self.args = args
        self.port = port
        self.host = host
        self.last = ''
        self.autosave = autosave
        self.chunk_size = chunk_size
        self.process_npy = process_npy
        self.store = store
        self.messages = []
        self.dedup_list = u.LimitStrList(1000)
        self.message_count = 0
        self.first_time = None
        self.define_vars('host', 'port', 'last', 'store', 'message_count', 'autosave', 'chunk_size', 'process_npy')
        self.define_event('on_udp_message')
        self.define_event('on_line')
        if start:
            self.start_server()

    def start_server(self):
        self.UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.UDPServerSocket.bind((self.host, self.port))
        self.delayed(1, self.tick)

    def on_udp_message(self, message, sender_ip):
        self.last = message.decode('utf-8')
        self.message_count += 1
        if self.first_time is None:
            self.first_time = time.time()
        if self.store:
            self.store_message(message, sender_ip)
            if self.autosave and self.message_count % self.chunk_size == 0:
                self.save(dict(save=f'{int(time.time())}.udp'))
                self.messages = []
        if self.process_npy:
            self.npy_message(message.decode('utf-8'), sender_ip)
        self.emit_event('on_udp_message', message=message.decode('utf-8'), ip=sender_ip)
        self.emit_event('on_line', line=message.decode('utf-8'))

    def npy_message(self, message, sender_ip='255.255.255.255'):
        if message.startswith('{'):
            with self.opt_dispatcher() as d:
                data = json.loads(message)
                if 'guid' in data:
                    if not data['guid'] in self.dedup_list:
                        self.dedup_list.append(data['guid'])
                    else:
                        return
                self.info('process ' + message)
                result = d.execute(data)
                if 'guid' in data:
                    nx.udp_send(json.dumps(result), sender_ip, self.port)

    def store_message(self, message, sender_ip):
        self.messages.append({
            'message': message.decode('utf-8'),
            'time': time.time(),
            'ip': sender_ip,
            'delta': time.time() - self.first_time
        })

    def tick(self):
        while True:
            try:
                (message, sender_ip) = self.UDPServerSocket.recvfrom(1024)
                self.info(str(message))
                self.on_udp_message(message, sender_ip)
            except Exception as e:
                self.error(str(e))

    def execute(self, data):
        if 'save' in data:
            return self.save(data)
        elif 'enable_store' in data:
            self.store = True
            return self.success()
        elif 'enable_npy' in data:
            self.process_npy = True
            return self.success()
        elif 'emit_file' in data:
            return self.emit_file(data)
        return super().execute(data)

    def save(self, data):
        filename = data['save']
        with open(filename, 'w') as f:
            json.dump(self.messages, f)
        return self.success('saved')

    def part_index(self, p: b.Page, params={}):
        p.prop("Message-Count", self.message_count)
        p.div(w.a("Save", f'?type={self.type}&{self.type}=save'))
        p.div(w.a("Last", f'?type={self.type}&{self.type}=last'))
        p(self.exec_btn("Enable Store", type=self.type, enable_store=1))
        p(self.exec_btn("Enable Npy", type=self.type, enable_npy=1))

    def part_last(self, p: b.Page, params={}):
        p.pre(json.dumps(self.messages[-10:], indent=2))

    def part_save(self, p: b.Page, params={}):
        p.input('filename', id='filename')
        p(w.button_js("Save", 'exec_job_p({"type": "' + self.type + '", "save": "#filename"})'))
        p.div('', id='result')

    def emit_file(self, data):
        filename = data['emit_file']
        with open(filename, 'r') as f:
            array = json.load(f)
            for item in array:
                self.emit_event('on_udp_message', message=item['message'], ip=item['ip'])
                time.sleep(item['delta'])
            return self.success('emited')

    def part_emit(self, p: b.Page, params={}):
        p.input('filename', id='filename')
        p(w.button_js("Save", 'exec_job_p({"type": "' + self.type + '", "emit_file": "#filename"})'))
        p.div('', id='result')