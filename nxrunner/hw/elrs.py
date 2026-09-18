import time

from nwebclient import runner as r, base
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d

import json


class Binding:
    channel: int
    mode: str
    value_name: str
    job_data: dict

    def __init__(self, data):
        self.channel = data['channel']
        self.mode = data['mode']
        self.value_name = data['value_name']
        self.job_data = u.load_to_dict(data['job_data'])

    @staticmethod
    def map_clamped(value, in_min, in_max, out_min, out_max):
        if in_min < in_max:
            value = max(in_min, min(in_max, value))
        else:
            value = max(in_max, min(in_min, value))

        return out_min + (
                (value - in_min) *
                (out_max - out_min) /
                (in_max - in_min)
            )

    @classmethod
    def from_dict(cls, data):
        return cls(
            channel=data["channel"],
            mode=data["mode"],
            value_name=data["value_name"],
            job_data=data["job_data"]
        )

    def execute_from_array(self, channels, job_callback):
        pwm = channels[self.channel]

        if self.mode == "PWM_H":
            # 1100 -> 0%  # 1900 -> 100%
            value = self.map_clamped(pwm, 1100, 1900, 0, 100)
        elif self.mode == "PWM_L":
            # 900 -> 0%   # 100 -> 100%
            value = self.map_clamped(pwm, 900, 100, 0, 100)
        elif self.mode == "PWM_F":
            # 100 -> 0%   # 1900 -> 100%
            value = self.map_clamped(pwm, 100, 1900, 0, 100)
        elif self.mode == "DIR_H":
            # >= 1100 -> 1 # < 1100  -> 0
            value = 1 if pwm >= 1100 else 0
        elif self.mode == "DIR_L":
            # 0..900 -> 1  # >900   -> 0
            value = 1 if 0 <= pwm <= 900 else 0

        job_callback({**self.job_data, **{self.value_name: value}})

    def to_html(self):
        return f"""
        <div class="binding">
            <div class="binding-channel">Channel {self.channel}</div>
            <div class="binding-mode">{self.mode}</div>
            <div class="binding-value">{self.value_name}</div>
        </div>
        """


class ElrsDisplay(r.BaseJobExecutor):

    def __init__(self, args: u.Args = {}):
        super().__init__('elrs_display')
        self.channels = [-1, 512,512,512,512,512, 512,512,512,512,512, 512]
        self.bindings = list(map(Binding.from_dict, args.get('elrs_bindings', [])))
        self.auto_fetch_interval = 1
        self.threads = {}

    def execute(self, data):
        for i in range(1, 11):
            if 'channel' + str(i) in data:
                self.channels[i] = data['channel' + str(i)]
                return self.success()
        return super().execute(data)

    def execute_process_line(self, data):
        s = data['value']
        exec_data = u.parse_query_string(s)
        return self.execute(exec_data)

    def execute_fetch(self, data):
        self.fetch(data['ip'])
        return self.success()

    def execute_auto_fetch(self, data):
        self.threads['auto_fetch'] = self.periodic(self.auto_fetch_interval, lambda: self.fetch(data['ip']))
        return self.success()

    def motor_esp_i2c_drive(self, ip, channel=2):
        # `/motor/set` | GET | `motor`, `speed`
        v = Binding.map_clamped(self.channels[channel], 200, 1800, -255, 255)
        if -20 < int(v) < 20:
            v = 0
        u.wget(f"http://{ip}/motor_set?motor=0&speed={v}")

    def execute_bind_esp_motor(self, data):
        channel = data.get('channel', 2)
        n = f"esp_motor_{channel}"
        self.threads[n] = self.periodic(self.auto_fetch_interval, lambda: self.motor_esp_i2c_drive(data['ip'], channel))
        return self.success(thread=n)

    def build_message(self):
        return json.dumps(dict(
            channel1=self.channels[1],
            channel2=self.channels[2],
            channel3=self.channels[3],
            channel4=self.channels[4],
            channel5=self.channels[5],
            channel6=self.channels[6],
            channel7=self.channels[7],
            channel8=self.channels[8],
            channel9=self.channels[9],
            channel10=self.channels[10],
            channel11=self.channels[11]
        ))

    def page_stream(self, params={}):
        import flask
        def stream():
            while True:
                time.sleep(1)
                yield f"data: {self.build_message()}\n\n"
        return flask.Response(stream(), mimetype='text/event-stream', headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    def fetch(self, ip, item_name="ElrsBinding4"):
        for i in range(1, 12):
            self.channels[i] = int(u.wget(f'http://{ip}/val?name={item_name}&value=ch{i}'))

    def build_script(self):
        url = self.link(self.page_stream)
        return f"""  const eventSource = new EventSource("{url}");
        
        eventSource.onmessage = function(event) {{
            try {{
                const data = JSON.parse(event.data);
                for (let channel = 1; channel <= 11; channel++) {{
                    const id = "channel" + channel;
                    const div = document.getElementById(id);
        
                    if (div && data[id] !== undefined) {{
                        div.textContent = data[id];
                    }}
                }}
            }} catch (error) {{
                console.error("SSE JSON Fehler:", error, event.data);
            }}
        }};
        
        eventSource.onerror = function(error) {{
            console.error("SSE Verbindungsfehler:", error);
        }}; """

    def part_index(self, p: base.Page, params={}):
        p.script(self.build_script())
        for i in range(1, 12):
            p.div(f"Channel{i}: ")
            p.div("-", id='channel'+str(i))

        p.hr()
        p.form_input("ip", "IP", id='ip')
        p(self.action_btn_parametric("Fetch", dict(
            type=self.type,
            op='fetch',
            ip='#ip'
        )))
        p(self.action_btn_parametric("Auto Fetch", dict(
            type=self.type, op='auto_fetch', ip='#ip'
        )))
        p(self.action_btn_parametric("Bind ESP-I2C Motor", dict(
            type=self.type, op='bind_esp_motor', ip='#ip'
        )))
        p.ul([
            w.a("Bindings", self.link(self.part_bindings))
        ])

    def execute_binding(self, data):
        channel = data['channel']
        mode = data['mode']
        value_name = data['value_name']
        job_data = data['job_data']

    def part_bindings(self, p: base.Page, params={}):
        p.form_input("channel", "Channel", id='channel')
        p.ul([
            "`PWM_H` ab 1100 pwm von 0% bis 100% bei 1900",
            "`PWM_L` ab 900 pwm 0% bis 100% bei 100",
            "`PWM_F` ab 100 pwm 0% bis 100% bei 1900",
            "`DIR_H` ab 1100 Pegel 1 sonst 0",
            "`DIR_L` ab 900 bis 0 Pegel 1 sonst pegel 0"
        ])
        p.form_input("mode", "Mode", id='mode')
        p.form_input("value_name", "Value Name", id='value_name')
        p.form_input("job_data", "Job Data", id='job_data')
        p(self.action_btn_parametric("Create Binding", dict(
            type=self.type, op='binding',
            mode='#mode', value_name='#value_name', job_data='#job_data', channel='#channel'
        )))
        p.hr()
        p.ul([
            w.a("Threads", self.link(self.part_threads))
        ])
        p("".join(list(map(lambda x: x.to_html(), self.bindings))))

    def execute_stop_thread(self, data):
        self.threads[data['thread_name']].stop()
        return self.success()

    def part_threads(self, p: b.Page, params={}):
        for k, v in self.threads.items():
            p.div(k + self.action_btn(dict(title="Stop", op='stop_thread', thread_name=k)))
