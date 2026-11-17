import html
import json
import time
import urllib.parse
from typing import List, Optional

from nwebclient import runner as r, base
from nwebclient import util as u
from nwebclient import base as b
from nwebclient import web as w
from nwebclient import dev


class TimelineItem(b.DictProxy):
    TIME_PX_FACTOR = 100

    def __init__(self, start=0, job='', timeline=None, duration=None):
        super().__init__({})  # eigenes dict, der Default von DictProxy wird sonst von allen Items geteilt
        if isinstance(start, dict):
            self._init_from_dict(start)
        elif isinstance(start, str) and not start.replace('.', '', 1).isnumeric():
            self._init_from_str(start)
        else:
            self.start = float(start)
            self.job = job
            self.duration = to_duration(duration)
        self.owner = timeline
        self.guid = u.guid()

    def _init_from_str(self, s: str):
        a = s.split('?', 1)
        self.start = float(a[0])
        self.job = '?' + a[1] if len(a) > 1 else ''
        self.duration = None

    def _init_from_dict(self, d: dict):
        self.start = float(d.get('start', 0))
        self.job = d.get('job', '')
        self.duration = to_duration(d.get('duration'))

    def get_end(self) -> float:
        return self.start + (self.duration or 0)

    def get_start_px(self):
        return self.start * self.TIME_PX_FACTOR

    def get_duration_css(self):
        if self.duration is None:
            return ''
        else:
            px = self.duration * self.TIME_PX_FACTOR
            return f'width: {px}px;'

    def get_owner_name(self):
        return self.owner.type

    def write_to(self, p: b.Page):
        caption = w.span(str(self.start), _class="time") + ": " + str(self.job)
        if self.owner is not None:
            caption += '<br />' + w.a("Edit", self.owner.link('edititem', guid=self.guid))
        p.div(caption,
              _class='timeline_item', id=f'i{id(self)}',
              style=f'left: {self.get_start_px()}px;' + self.get_duration_css(),
              data_pid=id(self), data_owner=self.get_owner_name())

    def to_dict(self):
        return {**self.obj, 'guid': self.guid, 'start': self.start, 'job': self.job, 'duration': self.duration}


def to_duration(value) -> Optional[float]:
    """ Dauer in s, None/''/0 bedeuten "ohne Dauer" """
    if value is None or value == '' or value == 'null':
        return None
    value = float(value)
    return value if value > 0 else None


class Timeline(r.BaseJobExecutor):
    """
      Liste von Jobs mit Startzeit (s), die nacheinander abgespielt werden.

      Bearbeitung grafisch ueber Scene.page_edit
    """

    items: u.List

    def __init__(self, type='timeline', items=[]):
        super().__init__(type)
        self.define_sig(dev.PInt('run', 1)).set_description('Spielt die Timeline ab').r(success=bool, message=str)
        self.define_sig(dev.PInt('run_async', 1)).set_description('Spielt die Timeline im Hintergrund ab').r(success=bool, message=str)
        self.define_sig(dev.Param('add', dict)).set_description('Hängt einen Job am Ende der Timeline an').r(success=bool, message=str)
        self.define_sig(dev.PInt('set_start_time', 1), dev.Param('item', int), dev.Param('start', float)).set_description('Setzt die Startzeit (s) des Eintrags mit der id item').r(success=bool, message=str)
        self.define_sig(dev.PInt('add_item', 1), dev.Param('start', float), dev.Param('job', str), dev.Param('duration', float)).set_description('Fügt einen Eintrag zur Startzeit start hinzu').r(success=bool, message=str, guid=str)
        self.define_sig(dev.PInt('update_item', 1), dev.Param('item', str), dev.Param('start', float), dev.Param('job', str), dev.Param('duration', float)).set_description('Ändert Startzeit, Job oder Dauer eines Eintrags').r(success=bool, message=str)
        self.define_sig(dev.PInt('remove_item', 1), dev.Param('item', str)).set_description('Entfernt einen Eintrag').r(success=bool, message=str)
        self.items = u.List()
        self.add_items(items)

    def get_max_time(self):
        return self.items.max('start')

    def get_duration(self) -> float:
        """ Ende des letzten Eintrags in s """
        return max([0, *map(lambda i: i.get_end(), self.items)])

    def find_item(self, ref) -> Optional[TimelineItem]:
        """ :param ref: guid oder id() des Eintrags """
        for item in self.items:
            if str(ref) == item.guid or str(ref) == str(id(item)):
                return item
        return None

    def add(self, item):
        if isinstance(item, TimelineItem):
            item.owner = self
            self.items.append(item)
        elif isinstance(item, dict) and 'job' in item:
            self.items.append(TimelineItem(item, timeline=self))
        elif isinstance(item, str) and '?' in item and not item.startswith('?'):
            self.items.append(TimelineItem(item, timeline=self))
        else:
            self.items.append(TimelineItem(self.get_max_time() + 1, item, timeline=self))
        return self.success('added')

    def add_items(self, items):
        for item in items:
            self.add(item)

    def add_item(self, data: dict):
        item = TimelineItem(data.get('start', self.get_duration()), data.get('job', ''), timeline=self,
                            duration=data.get('duration'))
        self.items.append(item)
        return self.success('added', guid=item.guid)

    def update_item(self, data: dict):
        item = self.find_item(data.get('item'))
        if item is None:
            return self.fail('item not found')
        if 'start' in data:
            item.start = max(0.0, float(data['start']))
        if 'duration' in data:
            item.duration = to_duration(data['duration'])
        if 'job' in data:
            item.job = data['job']
        return self.success('ok')

    def remove_item(self, data: dict):
        item = self.find_item(data.get('item'))
        if item is None:
            return self.fail('item not found')
        self.items.items.remove(item)
        return self.success('removed')

    def sorted_items(self) -> List[TimelineItem]:
        return sorted(self.items, key=lambda i: i.start)

    def run_item(self, item):
        if not item.job:
            return
        self.info('Starting ' + str(item.job))
        self.onParentClass(r.LazyDispatcher, lambda d: d.execute(item.job))

    def run_async(self):
        self.delayed(0, self.run)
        return self.success('started')

    def run(self):
        current_time = 0
        for item in self.sorted_items():
            sleep_time = max(0, item.start - current_time)
            time.sleep(sleep_time)
            current_time += sleep_time
            self.run_item(item)
        return self.success('ok')

    def set_start_time(self, data):
        item = self.find_item(data['item'])
        if item is not None:
            item.start = float(data['start'])
        return self.success('ok')

    def execute(self, data):
        if 'run' in data:
            return self.run()
        if 'run_async' in data:
            return self.run_async()
        if 'add' in data:
            return self.add(data['add'])
        elif 'set_start_time' in data:
            return self.set_start_time(data)
        elif 'add_item' in data:
            return self.add_item(data)
        elif 'update_item' in data or 'update' in data:
            return self.update_item(data)
        elif 'remove_item' in data:
            return self.remove_item(data)
        elif 'run_item' in data:
            item = self.find_item(data.get('item'))
            if item is None:
                return self.fail('item not found')
            self.run_item(item)
            return self.success('started')
        return super().execute(data)

    def part_add(self, p: base.Page, params={}):
        with self.opt_dispatcher() as d:
            for runner in d:
                p.div(runner.__class__.__name__)
                for sig in runner.signatures:
                    if sig.is_direct_callable():
                        btn = self.action_btn(title="Add", type=self.type, add=sig.to_q())
                        p.div(runner.type + ': ' + sig.for_npy() + btn, _class='op')
                    else:
                        p.div(runner.type + ': ' + sig.for_npy(), _class='op')

    def show_timeline(self, p: base.Page):
        p(f'<div class="timeline_items" id="i{id(self)}" title="{self.type}">')
        for item in self.items:
            item.write_to(p)
        p(self.action_btn(dict(title="Add", type=self.type, add='?title=new', ui_reload=1)))
        p('</div>')

    def css(self):
        # language=css
        return """
            .timeline_items { height: 200px; background-color: darkslateblue; position: relative; }
            .timeline_item { height: 195px; min-width: 10px; top: 0px; background-color: blueviolet; position: absolute; }
        """

    def part_index(self, p: base.Page, params={}):
        p(self.action_btn(dict(title="Run", run_async=1, type=self.type)))
        p.hr()
        self.show_timeline(p)
        p.right(w.a("Add Assitant", self.link(self.part_add)))
        p.hr()
        p.form_input('item', "Item (3:?type=xx&param=value)", id='item')
        p(self.action_btn_parametric("Add Item", dict(type=self.type, add='#item')))
        p.pre('', id='result')

    def part_edititem(self, p: base.Page, params={}):
        guid = params['guid']
        item = self.find_item(guid)
        p.form_input('job', "Job", id='job')
        i = 0
        js = ''
        with (self.opt_dispatcher() as d):  # type: r.LazyDispatcher
            for c in d.get_direct_callables():
                p.div(str(c.to_dict()), id=f'a{i}')
                js += '$("#a' + str(i) + '").click(function() {   $("#job").val("' + w.ql(c.to_dict()) + '"); });\n'
                i += 1
        p.js_ready(js)

    def to_dict(self):
        return {
            'name': self.type,
            'duration': self.get_duration(),
            'items': [itm.to_dict() for itm in self.sorted_items()]
        }


class Scene(r.BaseJobExecutor):
    """
      Mehrere Timelines, die gemeinsam abgespielt werden.

      nxrunner.animate:Scene

      Konfiguration (optional):

        scene:
          timelines:
            - name: licht
              items:
                - start: 0
                  job: ?type=gpio&pin=17&value=1
                  duration: 2
                - "4?type=gpio&pin=17&value=0"

      Bearbeitung: page_edit (?type=scene&scene=edit)

      @see https://github.com/Zulko/moviepy
    """

    def __init__(self, type='scene', args: u.Args = {}):
        super().__init__(type)
        self.items: List[Timeline] = []
        self.define_sig(dev.PInt('start', 1)).set_description('Startet alle Timelines im Hintergrund').r(success=bool, message=str)
        self.define_sig(dev.PInt('add', 1)).set_description('Fügt eine neue Timeline hinzu').r(success=bool, message=str, name=str)
        self.define_sig(dev.PInt('get_data', 1)).set_description('Alle Timelines mit ihren Einträgen').r(success=bool, timelines=list)
        self.define_sig(dev.Param('remove_timeline', str)).set_description('Entfernt eine Timeline').r(success=bool, message=str)
        if type in args:
            self.load_data(args[type])

    def get_connections(self) -> list:
        return [*map(lambda t: dict(type=t.type), self.items), *super().get_connections()]

    def save(self, name):
        import nweb
        with self.opt_nweb() as nw:  # type: nweb.NWeb
            self.to_dict()  # TODO as nweb

    def load_data(self, data):
        """ :param data: {'timelines': [{'name': ..., 'items': [...]}]} oder direkt die Liste """
        if isinstance(data, dict):
            data = data.get('timelines', [])
        for tl_data in data or []:
            tl = self.add_timeline(tl_data.get('name'))
            self.get_timeline(tl['name']).add_items(tl_data.get('items', []))

    def load(self, name):
        with self.opt_nweb() as nweb:
            pass  # TODO load

    def onOwnerChanged(self, newOnwer):
        self.info("New Owner:" + str(newOnwer))
        # Timelines aus der Konfiguration sind vor dem Dispatcher entstanden
        with self.opt_dispatcher() as d:
            for tl in self.items:
                if not d.support_type(tl.type):
                    d.add_runner(tl)

    def new_timeline_name(self):
        i = len(self.items) + 1
        while self.get_timeline('timeline' + str(i)) is not None:
            i += 1
        return 'timeline' + str(i)

    def get_timeline(self, name) -> Optional[Timeline]:
        for tl in self.items:
            if tl.type == name:
                return tl
        return None

    def get_duration(self) -> float:
        return max([0, *map(lambda t: t.get_duration(), self.items)])

    def add_timeline(self, name=None):
        if not name or self.get_timeline(name) is not None:
            name = self.new_timeline_name()
        tline = Timeline(name)
        self.items.append(tline)
        with self.opt_dispatcher() as d:
            d.add_runner(tline)
        return self.success(name=name)

    def remove_timeline(self, name):
        tl = self.get_timeline(name)
        if tl is None:
            return self.fail('timeline not found')
        self.items.remove(tl)
        with self.opt_dispatcher() as d:
            d.remove({'remove': name})
        return self.success('removed')

    def start(self, data={}):
        for tl in self.items:
            tl.run_async()
        return self.success()

    def timeline_job(self, data: dict):
        """ Leitet einen Job an die Timeline data['tl'] weiter (der Editor spricht nur mit der Scene) """
        tl = self.get_timeline(data['tl'])
        if tl is None:
            return self.fail('timeline not found')
        job = {k: v for k, v in data.items() if k not in ('tl', 'type')}
        return tl.execute(job)

    def with_data(self, result: dict) -> dict:
        """ Haengt den aktuellen Stand an ein Ergebnis an, damit der Editor neu zeichnen kann """
        return {**result, 'timelines': self.to_dict()['timelines'], 'duration': self.get_duration()}

    def execute(self, data):
        if 'tl' in data:
            return self.with_data(self.timeline_job(data))
        elif 'get_data' in data:
            return self.with_data(self.success())
        elif 'add' in data:
            return self.with_data(self.add_timeline(data.get('name')))
        elif 'remove_timeline' in data:
            return self.with_data(self.remove_timeline(data['remove_timeline']))
        elif 'start' in data:
            return self.start(data)
        return super().execute(data)

    def part_index(self, p: base.Page, params={}):
        count = sum(len(tl.items) for tl in self.items)
        p.div(f"{len(self.items)} Timelines &middot; {count} Einträge &middot; Dauer {self.get_duration():g} s")
        for tl in self.items:
            p.prop(tl.type, f"{len(tl.items)} Einträge, {tl.get_duration():g} s")
        p.div(self.action_btn(dict(title="Start", type=self.type, start=1)) + ' ' +
              w.a("Bearbeiten", self.dispatcher_link(self.page_edit)))

    def job_suggestions(self) -> List[dict]:
        """ Direkt aufrufbare Signaturen aller geladenen Runner als Vorschlaege fuer Jobs """
        res = []
        d = self.getParentClass(r.LazyDispatcher)
        if d is None:
            return res
        for runner in d.loaded():
            if isinstance(runner, (Timeline, Scene)):
                continue
            for sig in runner.signatures:
                try:
                    if sig.is_direct_callable():
                        job = {'type': runner.type, **{k: v for k, v in sig.job().items() if v is not None}}
                        res.append({'label': runner.type + ': ' + sig.label(),
                                    'description': sig.description or '',
                                    'job': '?' + urllib.parse.urlencode(job)})
                except Exception as e:
                    self.debug(f"job_suggestions {runner.type}: {e}")
        return res

    def page_edit(self, params={}):
        p = b.Page(owner=self)
        p['title'] = 'Scene Editor'
        p.style(EDITOR_CSS)
        p(EDITOR_HTML.replace('{index_link}', html.escape(self.dispatcher_link('index'))))
        config = {
            'type': self.type,
            'data': self.with_data({}),
            'jobs': self.job_suggestions(),
        }
        p.script('const SCENE_CONFIG = ' + json.dumps(config).replace('</', '<\\/') + ';')
        p.script(EDITOR_JS)
        return p.nxui()

    def to_dict(self):
        return dict(
            timelines=[tl.to_dict() for tl in self.items]
        )


# language=css
EDITOR_CSS = """
    .scn { --scn-head: 170px; --scn-row: 56px; --scn-bg: #1d1f27; --scn-lane: #262a35; --scn-line: #3a3f4d;
           --scn-text: #e6e8ee; --scn-muted: #8b91a1; --scn-accent: #7c5cff; --scn-sel: #ffcf5c;
           color: var(--scn-text); background: var(--scn-bg); border-radius: 8px; font-size: 13px;
           display: flex; flex-direction: column; min-height: 60vh; user-select: none; }
    .scn button, .scn select, .scn input { font: inherit; }
    .scn-toolbar { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; padding: 8px 10px;
                   border-bottom: 1px solid var(--scn-line); }
    .scn-toolbar .sp { flex: 1; }
    .scn-btn { background: #333848; color: var(--scn-text); border: 1px solid var(--scn-line); border-radius: 5px;
               padding: 4px 10px; cursor: pointer; }
    .scn-btn:hover { background: #3f4558; }
    .scn-btn.primary { background: var(--scn-accent); border-color: var(--scn-accent); }
    .scn-btn.danger:hover { background: #a33; }
    .scn-btn.small { padding: 1px 6px; font-size: 12px; }
    .scn-toolbar label { color: var(--scn-muted); display: flex; gap: 4px; align-items: center; }
    .scn-time { font-family: monospace; min-width: 70px; text-align: right; }
    .scn-body { display: flex; flex: 1; min-height: 0; }
    .scn-tracks { flex: 1; overflow: auto; position: relative; }
    .scn-grid { position: relative; min-width: 100%; }
    .scn-row { display: flex; height: var(--scn-row); border-bottom: 1px solid var(--scn-line); }
    .scn-row.ruler { height: 26px; position: sticky; top: 0; z-index: 3; background: var(--scn-bg); }
    .scn-head { width: var(--scn-head); flex: none; position: sticky; left: 0; z-index: 2; background: var(--scn-bg);
                border-right: 1px solid var(--scn-line); padding: 4px 8px; box-sizing: border-box;
                display: flex; flex-direction: column; justify-content: center; gap: 3px; overflow: hidden; }
    .scn-head .name { font-weight: bold; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .scn-head > * { flex: none; }
    .scn-head .ops { display: flex; gap: 3px; align-items: center; }
    .scn-head .count { color: var(--scn-muted); font-size: 12px; margin-right: auto; }
    .scn-lane { position: relative; flex: none; background: var(--scn-lane); cursor: copy;
                background-image: repeating-linear-gradient(90deg, transparent 0 calc(var(--pps) * 1px - 1px),
                                  #ffffff0d calc(var(--pps) * 1px - 1px) calc(var(--pps) * 1px)); }
    .scn-ruler { position: relative; flex: none; cursor: pointer; }
    .scn-tick { position: absolute; top: 0; bottom: 0; border-left: 1px solid var(--scn-line);
                color: var(--scn-muted); font-size: 11px; padding-left: 3px; line-height: 26px; }
    .scn-item { position: absolute; top: 6px; bottom: 6px; min-width: 16px; box-sizing: border-box;
                background: var(--scn-accent); border-radius: 4px; padding: 2px 8px 2px 6px; cursor: grab;
                overflow: hidden; white-space: nowrap; text-overflow: ellipsis; font-size: 12px;
                box-shadow: 0 1px 3px #0006; }
    .scn-item.instant { background: transparent; border-left: 3px solid var(--scn-accent); border-radius: 0;
                        box-shadow: none; max-width: 180px; color: var(--scn-text); }
    .scn-item.instant .t { color: var(--scn-muted); }
    .scn-item.selected { outline: 2px solid var(--scn-sel); z-index: 1; }
    .scn-item.instant.selected { border-left-color: var(--scn-sel); outline: none; }
    .scn-item.dragging { cursor: grabbing; opacity: .85; }
    .scn-item .t { font-family: monospace; opacity: .8; margin-right: 4px; }
    .scn-item .rs { position: absolute; right: 0; top: 0; bottom: 0; width: 7px; cursor: ew-resize; }
    .scn-item .rs:hover { background: #fff3; }
    .scn-playhead { position: absolute; top: 0; bottom: 0; width: 2px; background: #ff5c7a; z-index: 4;
                    pointer-events: none; display: none; }
    .scn-empty { padding: 30px; color: var(--scn-muted); text-align: center; }
    .scn-inspector { width: 300px; flex: none; border-left: 1px solid var(--scn-line); padding: 10px;
                     box-sizing: border-box; overflow: auto; user-select: text; }
    .scn-inspector h3 { margin: 0 0 8px 0; font-size: 14px; }
    .scn-inspector .hint { color: var(--scn-muted); line-height: 1.5; }
    .scn-field { display: flex; flex-direction: column; gap: 3px; margin-bottom: 8px; }
    .scn-field span { color: var(--scn-muted); font-size: 12px; }
    .scn-field input, .scn-field textarea, .scn-search { background: #14161c; color: var(--scn-text);
                     border: 1px solid var(--scn-line); border-radius: 4px; padding: 4px 6px; }
    .scn-field textarea { font-family: monospace; min-height: 54px; resize: vertical; }
    .scn-pair { display: flex; gap: 8px; } .scn-pair .scn-field { flex: 1; min-width: 0; }
    .scn-actions { display: flex; gap: 6px; flex-wrap: wrap; margin: 4px 0 12px 0; }
    .scn-search { width: 100%; box-sizing: border-box; margin-bottom: 4px; }
    .scn-jobs { max-height: 260px; overflow: auto; border: 1px solid var(--scn-line); border-radius: 4px; }
    .scn-job { padding: 4px 6px; cursor: pointer; border-bottom: 1px solid var(--scn-line); }
    .scn-job:hover { background: #333848; }
    .scn-job small { display: block; color: var(--scn-muted); }
    .scn-status { color: var(--scn-muted); font-size: 12px; min-height: 16px; }
    .scn-status.err { color: #ff7a7a; }
    @media (max-width: 800px) { .scn-body { flex-direction: column; } .scn-inspector { width: auto; border-left: 0;
        border-top: 1px solid var(--scn-line); } .scn { --scn-head: 110px; } }
"""

EDITOR_HTML = """
<div class="scn" id="scn" tabindex="0">
  <div class="scn-toolbar">
    <button class="scn-btn primary" id="scn-play" title="Alle Timelines starten">&#9654; Start</button>
    <span class="scn-time" id="scn-time">0.0 s</span>
    <button class="scn-btn" id="scn-add-tl">+ Timeline</button>
    <span class="sp"></span>
    <label>Raster <select id="scn-snap">
      <option value="0">aus</option><option value="0.1">0.1 s</option><option value="0.25">0.25 s</option>
      <option value="0.5" selected>0.5 s</option><option value="1">1 s</option></select></label>
    <label>Zoom <input type="range" id="scn-zoom" min="20" max="400" step="10" value="100"></label>
    <a class="scn-btn" href="{index_link}">&larr; Übersicht</a>
  </div>
  <div class="scn-body">
    <div class="scn-tracks" id="scn-tracks"><div class="scn-grid" id="scn-grid"></div></div>
    <aside class="scn-inspector" id="scn-inspector"></aside>
  </div>
  <div class="scn-status" id="scn-status" style="padding: 4px 10px"></div>
</div>
"""

# language=javascript
EDITOR_JS = """
(function() {
    const cfg = SCENE_CONFIG;
    const state = {
        data: cfg.data, selected: null,
        pps: +(localStorage.getItem('scn_pps') || 100),
        snap: +(localStorage.getItem('scn_snap') || 0.5),
        playStart: null
    };
    const $ = (id) => document.getElementById(id);
    const el = (tag, cls, text) => { const e = document.createElement(tag); if (cls) e.className = cls;
                                     if (text !== undefined) e.textContent = text; return e; };
    const fmt = (s) => (Math.round(s * 100) / 100).toString();
    const snapT = (t) => { t = Math.max(0, t); return state.snap > 0 ? Math.round(t / state.snap) * state.snap : t; };

    function status(msg, err) { const s = $('scn-status'); s.textContent = msg || ''; s.classList.toggle('err', !!err); }

    function api(data) {
        return fetch(base_url(), post({type: cfg.type, ...data}))
            .then(r => r.json())
            .then(res => {
                if (res.timelines) { state.data = res; }
                if (res.success === false) { status(res.message || res.error || 'Fehler', true); }
                else { status(res.message && res.message !== 'ok' ? res.message : ''); }
                render();
                return res;
            })
            .catch(e => { status('Verbindungsfehler: ' + e, true); });
    }

    function findItem(guid) {
        for (const tl of state.data.timelines) for (const it of tl.items) if (it.guid === guid) return [tl, it];
        return [null, null];
    }

    function jobLabel(job) {
        if (!job) return '(leer)';
        if (typeof job !== 'string') return JSON.stringify(job);
        const q = new URLSearchParams(job.replace(/^\\?/, ''));
        const type = q.get('type'); q.delete('type'); q.delete('title');
        const rest = q.toString().replace(/&/g, ' ');
        return type ? type + (rest ? ': ' + rest : '') : job;
    }

    function laneWidth() {
        const tracks = $('scn-tracks');
        const head = parseInt(getComputedStyle($('scn')).getPropertyValue('--scn-head')) || 170;
        const visible = (tracks.clientWidth - head) / state.pps;
        return Math.max(visible, (state.data.duration || 0) + 5) * state.pps;
    }

    function tickStep() {
        for (const s of [0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60]) if (s * state.pps >= 60) return s;
        return 120;
    }

    function render() {
        const grid = $('scn-grid');
        grid.innerHTML = '';
        grid.style.setProperty('--pps', state.pps);
        const w = laneWidth();
        // Zeitleiste
        const rulerRow = el('div', 'scn-row ruler');
        rulerRow.appendChild(el('div', 'scn-head'));
        const ruler = el('div', 'scn-ruler'); ruler.style.width = w + 'px';
        const step = tickStep();
        for (let t = 0; t * state.pps < w; t += step) {
            const tick = el('div', 'scn-tick', fmt(t) + ' s'); tick.style.left = (t * state.pps) + 'px'; ruler.appendChild(tick);
        }
        rulerRow.appendChild(ruler);
        grid.appendChild(rulerRow);
        if (state.data.timelines.length === 0) {
            const empty = el('div', 'scn-empty', 'Noch keine Timeline. Mit "+ Timeline" anlegen.');
            grid.appendChild(empty);
        }
        for (const tl of state.data.timelines) grid.appendChild(renderRow(tl, w));
        const ph = el('div', 'scn-playhead'); ph.id = 'scn-playhead'; grid.appendChild(ph);
        renderInspector();
    }

    function renderRow(tl, w) {
        const row = el('div', 'scn-row');
        const head = el('div', 'scn-head');
        const name = el('div', 'name', tl.name); name.title = tl.name;
        head.appendChild(name);
        const ops = el('div', 'ops');
        ops.appendChild(el('span', 'count', tl.items.length + ' Einträge'));
        const add = el('button', 'scn-btn small', '+'); add.title = 'Eintrag am Ende hinzufügen';
        add.onclick = () => addItem(tl.name, tl.duration ? snapT(tl.duration + 1) : 0);
        const del = el('button', 'scn-btn small danger', '✕'); del.title = 'Timeline entfernen';
        del.onclick = () => { if (confirm('Timeline "' + tl.name + '" entfernen?')) api({remove_timeline: tl.name}); };
        ops.append(add, del);
        head.appendChild(ops);
        row.appendChild(head);

        const lane = el('div', 'scn-lane'); lane.style.width = w + 'px';
        lane.title = 'Doppelklick: neuer Eintrag';
        lane.addEventListener('dblclick', (e) => {
            if (e.target !== lane) return;
            addItem(tl.name, snapT(e.offsetX / state.pps));
        });
        lane.addEventListener('pointerdown', (e) => { if (e.target === lane) select(null); });
        for (const it of tl.items) lane.appendChild(renderItem(tl, it));
        row.appendChild(lane);
        return row;
    }

    function renderItem(tl, it) {
        const d = el('div', 'scn-item' + (it.duration ? '' : ' instant') + (state.selected === it.guid ? ' selected' : ''));
        d.style.left = (it.start * state.pps) + 'px';
        if (it.duration) d.style.width = (it.duration * state.pps) + 'px';
        const t = el('span', 't', fmt(it.start));
        d.append(t, document.createTextNode(jobLabel(it.job)));
        d.title = fmt(it.start) + ' s' + (it.duration ? ' – ' + fmt(it.start + it.duration) + ' s' : '') + '\\n' + (it.job || '');
        const rs = el('div', 'rs'); rs.title = 'Dauer ändern';
        d.appendChild(rs);
        d.addEventListener('pointerdown', (e) => startDrag(e, tl, it, d, e.target === rs ? 'resize' : 'move'));
        return d;
    }

    function startDrag(e, tl, it, node, mode) {
        e.preventDefault(); e.stopPropagation();
        select(it.guid, false);
        node.classList.add('selected', 'dragging');
        node.setPointerCapture(e.pointerId);
        const x0 = e.clientX, start0 = it.start, dur0 = it.duration || 0;
        let moved = false, val = mode === 'move' ? start0 : dur0;
        const t = node.querySelector('.t');
        const onMove = (ev) => {
            const dt = (ev.clientX - x0) / state.pps;
            if (Math.abs(ev.clientX - x0) > 2) moved = true;
            if (!moved) return;
            if (mode === 'move') {
                val = snapT(start0 + dt);
                node.style.left = (val * state.pps) + 'px';
                t.textContent = fmt(val);
                $('scn-time').textContent = fmt(val) + ' s';
            } else {
                val = Math.max(0, snapT(start0 + dur0 + dt) - start0);
                node.classList.toggle('instant', val <= 0);
                node.style.width = val > 0 ? (val * state.pps) + 'px' : '';
                $('scn-time').textContent = 'Dauer ' + fmt(val) + ' s';
            }
        };
        const onUp = () => {
            node.removeEventListener('pointermove', onMove);
            node.removeEventListener('pointerup', onUp);
            node.removeEventListener('pointercancel', onUp);
            node.classList.remove('dragging');
            if (!moved) { renderInspector(); return; }
            const change = mode === 'move' ? {start: val} : {duration: val > 0 ? val : ''};
            update(tl.name, it.guid, change);
        };
        node.addEventListener('pointermove', onMove);
        node.addEventListener('pointerup', onUp);
        node.addEventListener('pointercancel', onUp);
    }

    function select(guid, rerender = true) {
        state.selected = guid;
        document.querySelectorAll('.scn-item.selected').forEach(n => n.classList.remove('selected'));
        if (rerender) render();
    }

    function addItem(tlName, start) {
        api({tl: tlName, add_item: 1, start: fmt(start), job: ''}).then(res => {
            if (res && res.guid) { state.selected = res.guid; render(); const j = $('scn-job'); if (j) j.focus(); }
        });
    }

    function update(tlName, guid, change) {
        return api({tl: tlName, update_item: 1, item: guid, ...change});
    }

    function field(label, input) {
        const f = el('label', 'scn-field'); f.appendChild(el('span', null, label)); f.appendChild(input); return f;
    }

    function renderInspector() {
        const box = $('scn-inspector');
        const [tl, it] = findItem(state.selected);
        box.innerHTML = '';
        if (!it) {
            box.appendChild(el('h3', null, 'Szene'));
            const n = state.data.timelines.reduce((a, t) => a + t.items.length, 0);
            box.appendChild(el('div', 'hint', state.data.timelines.length + ' Timelines, ' + n + ' Einträge, Dauer ' + fmt(state.data.duration || 0) + ' s'));
            const help = el('div', 'hint');
            help.style.marginTop = '12px';
            help.innerHTML = '<b>Bedienung</b><br>Doppelklick in eine Spur: neuer Eintrag<br>Ziehen: Startzeit ändern<br>' +
                'Rechte Kante ziehen: Dauer ändern<br>Entf: Eintrag löschen<br>&larr; / &rarr;: um ein Rasterschritt verschieben<br>' +
                'Strg + Mausrad: Zoom';
            box.appendChild(help);
            return;
        }
        box.appendChild(el('h3', null, 'Eintrag in ' + tl.name));
        const start = el('input'); start.type = 'number'; start.step = '0.1'; start.min = '0'; start.value = fmt(it.start);
        const dur = el('input'); dur.type = 'number'; dur.step = '0.1'; dur.min = '0'; dur.placeholder = 'ohne';
        dur.value = it.duration ? fmt(it.duration) : '';
        const pair = el('div', 'scn-pair'); pair.append(field('Start (s)', start), field('Dauer (s)', dur));
        box.appendChild(pair);
        const job = el('textarea'); job.id = 'scn-job'; job.value = it.job || ''; job.placeholder = '?type=runner&param=wert';
        box.appendChild(field('Job', job));

        const save = () => update(tl.name, it.guid, {start: start.value || 0, duration: dur.value, job: job.value});
        [start, dur].forEach(i => i.addEventListener('keydown', (e) => { if (e.key === 'Enter') save(); }));
        job.addEventListener('keydown', (e) => { if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) save(); });

        const actions = el('div', 'scn-actions');
        const bSave = el('button', 'scn-btn primary', 'Speichern'); bSave.onclick = save; bSave.title = 'Strg+Enter';
        const bRun = el('button', 'scn-btn', '▶ Ausführen'); bRun.title = 'Nur diesen Job jetzt ausführen';
        bRun.onclick = () => api({tl: tl.name, run_item: 1, item: it.guid});
        const bDel = el('button', 'scn-btn danger', 'Löschen'); bDel.onclick = () => removeSelected();
        actions.append(bSave, bRun, bDel);
        box.appendChild(actions);

        if (cfg.jobs.length > 0) {
            box.appendChild(el('div', 'scn-field', '')).appendChild(el('span', null, 'Job-Vorlagen'));
            const search = el('input', 'scn-search'); search.placeholder = 'Filtern…';
            const list = el('div', 'scn-jobs');
            const fill = () => {
                list.innerHTML = '';
                const q = search.value.toLowerCase();
                for (const j of cfg.jobs) {
                    if (q && !(j.label + ' ' + j.description).toLowerCase().includes(q)) continue;
                    const row = el('div', 'scn-job', j.label);
                    if (j.description) row.appendChild(el('small', null, j.description));
                    row.onclick = () => { job.value = j.job; save(); };
                    list.appendChild(row);
                }
            };
            search.addEventListener('input', fill);
            fill();
            box.append(search, list);
        }
    }

    function removeSelected() {
        const [tl, it] = findItem(state.selected);
        if (!it) return;
        state.selected = null;
        api({tl: tl.name, remove_item: 1, item: it.guid});
    }

    // Abspielen: Server startet alle Timelines, hier nur eine Abspielposition als Anzeige
    function play() {
        api({start: 1});
        state.playStart = performance.now();
        const dur = state.data.duration || 0;
        const tick = () => {
            const ph = $('scn-playhead');
            const t = (performance.now() - state.playStart) / 1000;
            const head = parseInt(getComputedStyle($('scn')).getPropertyValue('--scn-head')) || 170;
            if (t > dur + 0.5) { if (ph) ph.style.display = 'none'; $('scn-time').textContent = '0.0 s'; state.playStart = null; return; }
            if (ph) { ph.style.display = 'block'; ph.style.left = (head + t * state.pps) + 'px'; }
            $('scn-time').textContent = t.toFixed(1) + ' s';
            requestAnimationFrame(tick);
        };
        requestAnimationFrame(tick);
    }

    function setZoom(pps) {
        state.pps = Math.min(400, Math.max(20, pps));
        $('scn-zoom').value = state.pps;
        try { localStorage.setItem('scn_pps', state.pps); } catch (e) {}
        render();
    }

    $('scn-play').onclick = play;
    $('scn-add-tl').onclick = () => api({add: 1});
    $('scn-zoom').value = state.pps;
    $('scn-zoom').addEventListener('input', (e) => setZoom(+e.target.value));
    $('scn-snap').value = String(state.snap);
    $('scn-snap').addEventListener('change', (e) => {
        state.snap = +e.target.value; try { localStorage.setItem('scn_snap', state.snap); } catch (err) {}
    });
    $('scn-tracks').addEventListener('wheel', (e) => {
        if (!e.ctrlKey) return;
        e.preventDefault();
        setZoom(state.pps * (e.deltaY < 0 ? 1.15 : 1 / 1.15));
    }, {passive: false});
    document.addEventListener('keydown', (e) => {
        if (['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement.tagName)) return;
        const [tl, it] = findItem(state.selected);
        if (!it) return;
        if (e.key === 'Delete') { e.preventDefault(); removeSelected(); }
        else if (e.key === 'Escape') { select(null); }
        else if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
            e.preventDefault();
            const step = state.snap || 0.1;
            update(tl.name, it.guid, {start: fmt(Math.max(0, it.start + (e.key === 'ArrowLeft' ? -step : step)))});
        }
    });
    window.addEventListener('resize', render);
    render();
})();
"""


class VideoClip(r.BaseJobExecutor):
    """
    python -c "import nxrunner.animate as a; a.VideoClip('clip1', None).render()"

    videoclip:
        items:
            - text: Intro
              duration: 5
            - video: /data/clip1.mp4
            - video: /data/clip2.mp4
              start: 4
              stop: 8

    """
    def __init__(self, type='videoclip', args: u.Args = {}):
        super().__init__(type)
        if args is None:
            args = u.Args()
        self.args = args
        self.items = u.List()
        self.font = args.get('repos', {}).get('static', '') + 'fonts/gijgo-material.ttf'
        if type in args:
            self.load(args[type])

    def load(self, data={}):
        items = data.get('items', [])
        for item in items:
            if 'video' in item:
                self.add_clip(item['video'], **item)
            if 'text' in item:
                self.add_text(**item)

    def add_clip(self, path, **kwargs):
        from moviepy import VideoFileClip
        clip = VideoFileClip(path)
        #            .subclipped(10, 20)
        #    .with_volume_scaled(0.8)
        if 'start' in kwargs and 'stop' in kwargs:
            clip = clip.subclipped(int(kwargs['start']), int(kwargs['stop']))
        self.items.append(clip)

    def add_text(self, text, duration=5, font_size=70, color='white', font=None):
        from moviepy import TextClip
        txt_clip = TextClip(
            font=self.font if font is None else font,
            text=text,
            font_size=font_size,
            color=color
        ).with_duration(duration).with_position('center')
        self.items.append(txt_clip)

    def render(self, file='result.mp4'):
        from moviepy import CompositeVideoClip
        final_video = CompositeVideoClip(self.items.items)
        final_video.write_videofile(file)
