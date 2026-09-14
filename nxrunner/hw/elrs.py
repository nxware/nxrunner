from nwebclient import runner as r, base
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import dev as d

import json


class ElrsDisplay(r.BaseJobExecutor):

    def __init__(self):
        super().__init__('elrs_display')
        self.channels = [-1, 512,512,512,512,512, 512,512,512,512,512, 512]

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

    def page_stream(self, params={}):
        import flask
        def stream():
            while True:
                yield self.build_message()
        return flask.Response(stream(), mimetype='text/event-stream')

    def fetch(self, ip, item_name="ElrsBinding6"):
        for i in range(1, 11):
            self.channels[i] = u.wget(f'http://{ip}/val?name={item_name}&ch{i}')

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
        for i in range(1, 11):
            p.div(f"Channel{i}: ")
            p.div("-", id='channel'+str(1))

        p.hr()
        p.form_input("ip", "IP")
        p(self.action_btn_parametric("Fetch", dict(
            type=self.type,
            op='fetch',
            ip='#ip'
        )))
