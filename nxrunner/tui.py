"""
  Terminal-Oberflaeche fuer einen laufenden nxrunner (REST-Service von python -m nxrunner serv).

    python -m nxrunner tui [--url http://host:7070] [--user admin --password ...]

  Links die Runner, rechts je Runner die Operationen (Signaturen), die part_*-Seiten (TerminalPage)
  und ein freier JSON-Job. Ergebnisse unten im Log. Benoetigt textual (pip install nxrunner[tui]).
"""
import json

from rich.markup import MarkupError
from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widgets import (Button, Footer, Header, Input, Label, OptionList, RichLog, Static, TabbedContent,
                             TabPane, TextArea)
from textual.widgets.option_list import Option

from nxrunner.client import NxClient, NxClientError, format_result

CSS = """
#left { width: 32; border-right: solid $primary; }
#runners, #ops, #parts { height: 1fr; }
#ops, #parts { width: 32; border-right: solid $primary-darken-2; }
#form, #page { padding: 0 1; }
#tabs, #form, #page_col { width: 1fr; }
#page_bar { height: auto; max-height: 5; padding: 0 1; }
#page_links { height: 8; border-top: solid $primary-darken-2; }
#page_bar Button { margin: 0 1 0 0; min-width: 6; }
#log { height: 12; border-top: solid $primary; }
#job { height: 1fr; }
.sig_desc { color: $text-muted; margin-bottom: 1; }
.param_label { margin-top: 1; }
#form_buttons, #job_buttons { height: auto; margin-top: 1; }
"""


class ParamInput(Input):
    """ Eingabefeld fuer einen Parameter einer Signatur """

    def __init__(self, param: dict, value):
        super().__init__(value='' if value is None else str(value), placeholder=param.get('type') or '')
        self.param_name = param['name']
        self.tooltip = param.get('description') or param['name']


class JobButton(Button):
    """ Button, der einen Job ausfuehrt (Aktion einer TerminalPage) """

    def __init__(self, label: str, job: dict, hint: str = ''):
        super().__init__(label, variant='primary')
        self.job = job
        self.tooltip = hint or json.dumps(job, ensure_ascii=False)


def make_button(label: str, button_id: str, tooltip: str, variant='default') -> Button:
    button = Button(label, id=button_id, variant=variant)
    button.tooltip = tooltip
    return button


def link_prompt(number: int, link: dict) -> Text:
    text = Text(f"→{number} {link.get('title') or link['page']}  ")
    text.append(f"{link['runner']}/{link['page']}", style='dim')
    return text


def to_text(markup: str) -> Text:
    try:
        return Text.from_markup(markup)
    except MarkupError:
        return Text(markup)


class NxRunnerTui(App):
    TITLE = 'nxrunner'
    CSS = CSS
    BINDINGS = [
        ('ctrl+q', 'quit', 'Beenden'),
        Binding('f2', 'show_runners', 'Runner', priority=True),
        Binding('f3', 'show_tab("tab_ops", "#ops")', 'Operationen', priority=True),
        Binding('f4', 'show_tab("tab_pages", "#parts")', 'Seiten', priority=True),
        Binding('f6', 'reload', 'Neu laden', priority=True),
        ('ctrl+l', 'clear_log', 'Log leeren'),
    ]

    def __init__(self, client: NxClient):
        super().__init__()
        self.client = client
        self.all_runners = []
        self.runner = None  # gewaehlter Runner (type)
        self.signatures = []
        self.signature = None
        self.page_name = None
        self.page_params = {}
        self.page_links = []

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id='left'):
                yield Input(placeholder='Filter', id='filter')
                yield OptionList(id='runners')
            with TabbedContent(id='tabs'):
                yield from self.compose_ops_tab()
                yield from self.compose_pages_tab()
                yield from self.compose_job_tab()
        yield RichLog(id='log', wrap=True, markup=False, highlight=True)
        yield Footer()

    def compose_ops_tab(self) -> ComposeResult:
        with TabPane('Operationen', id='tab_ops'):
            with Horizontal():
                yield OptionList(id='ops')
                yield VerticalScroll(Label('Runner und Operation waehlen'), id='form')

    def compose_pages_tab(self) -> ComposeResult:
        with TabPane('Seiten', id='tab_pages'):
            with Horizontal():
                yield OptionList(id='parts')
                with Vertical(id='page_col'):
                    yield VerticalScroll(Static('Runner und Seite waehlen', id='page_text'), id='page')
                    yield Horizontal(id='page_bar')
                    yield OptionList(id='page_links')

    def compose_job_tab(self) -> ComposeResult:
        with TabPane('Job', id='tab_job'):
            with Vertical():
                yield TextArea('{\n  "type": "",\n  "op": ""\n}', language='json', id='job')
                with Horizontal(id='job_buttons'):
                    yield make_button('Senden', 'send_job', 'JSON-Job an den nxrunner senden', 'primary')

    def on_mount(self):
        self.sub_title = self.client.url
        self.load_runners()

    # --- Log --------------------------------------------------------------------------------

    def log_line(self, text: str, style: str = ''):
        self.query_one('#log', RichLog).write(Text(text, style=style))

    def log_result(self, title: str, result):
        self.log_line('▶ ' + title, 'bold')
        self.query_one('#log', RichLog).write(format_result(result))

    def log_error(self, e: Exception):
        self.log_line('✖ ' + str(e), 'bold red')

    def action_clear_log(self):
        self.query_one('#log', RichLog).clear()

    # --- Runner -----------------------------------------------------------------------------

    def action_show_runners(self):
        self.query_one('#runners', OptionList).focus()

    def action_show_tab(self, tab_id: str, focus: str):
        self.query_one('#tabs', TabbedContent).active = tab_id
        self.query_one(focus).focus()

    def action_reload(self):
        """ Runner-Liste, Operationen/Seiten des gewaehlten Runners und offene Seite neu laden """
        self.load_runners()
        if self.runner:
            self.reload_runner_details()
        self.action_reload_page()

    def reload_runner_details(self):
        self.query_one('#ops', OptionList).clear_options()
        self.query_one('#parts', OptionList).clear_options()
        self.load_runner_details(self.runner)

    @work(thread=True, exclusive=True, group='runners')
    def load_runners(self):
        try:
            name = self.client.ping()
            runners = self.client.runners()
        except NxClientError as e:
            self.call_from_thread(self.log_error, e)
            return
        self.call_from_thread(self.show_runners, name, runners)

    def show_runners(self, name: str, runners: list):
        self.sub_title = f"{name} – {self.client.url}"
        self.all_runners = runners
        self.filter_runners(self.query_one('#filter', Input).value)
        self.log_line(f"{len(runners)} Runner geladen", 'green')

    def filter_runners(self, text: str):
        text = text.strip().lower()
        runners = self.query_one('#runners', OptionList)
        runners.clear_options()
        for r in self.all_runners:
            if text in r['name'].lower() or text in str(r.get('cls', '')).lower():
                mark = '●' if r.get('loaded') else '○'
                runners.add_option(Option(f"{mark} {r['name']}", id=r['name']))

    def on_input_changed(self, event: Input.Changed):
        if event.input.id == 'filter':
            self.filter_runners(event.value)

    async def on_option_list_option_selected(self, event: OptionList.OptionSelected):
        list_id = event.option_list.id
        if list_id == 'runners':
            self.select_runner(event.option.id)
        elif list_id == 'ops':
            await self.select_signature(int(event.option.id))
        elif list_id == 'parts':
            self.open_page(event.option.id, {})
        elif list_id == 'page_links':
            self.follow_link(self.page_links[int(event.option.id)])

    def select_runner(self, name: str):
        self.runner = name
        self.signature = None
        self.page_name = None
        self.reload_runner_details()

    @work(thread=True, exclusive=True, group='details')
    def load_runner_details(self, name: str):
        try:
            sigs = self.client.signatures(name)
        except NxClientError as e:
            sigs = []
            self.call_from_thread(self.log_error, e)
        try:
            parts = self.client.parts(name)
        except NxClientError as e:
            parts = []
            self.call_from_thread(self.log_error, e)
        self.call_from_thread(self.show_runner_details, name, sigs, parts)

    def show_runner_details(self, name: str, sigs: list, parts: list):
        if name != self.runner:
            return
        self.signatures = sigs
        ops = self.query_one('#ops', OptionList)
        ops.clear_options()
        for i, sig in enumerate(sigs):
            ops.add_option(Option(sig.get('name') or sig.get('label') or f'#{i}', id=str(i)))
        self.query_one('#parts', OptionList).add_options([Option(p, id=p) for p in parts])
        self.log_line(f"{name}: {len(sigs)} Operationen, {len(parts)} Seiten", 'green')
        self.prefill_job(name)

    def prefill_job(self, name: str):
        area = self.query_one('#job', TextArea)
        try:
            job = json.loads(area.text)
        except ValueError:
            job = {}
        if isinstance(job, dict):
            area.text = json.dumps({**job, 'type': name}, indent=2, ensure_ascii=False)

    # --- Operationen ------------------------------------------------------------------------

    async def select_signature(self, index: int):
        self.signature = self.signatures[index]
        form = self.query_one('#form', VerticalScroll)
        await form.remove_children()
        await form.mount_all(self.signature_widgets(self.signature))

    def signature_widgets(self, sig: dict) -> list:
        widgets = [Label(f"[b]{sig.get('name') or sig.get('label')}[/b]")]
        if sig.get('description'):
            widgets.append(Static(sig['description'], classes='sig_desc'))
        job = sig.get('job') or {}
        for param in sig.get('params', []):
            hint = f" [dim]{param['description']}[/dim]" if param.get('description') else ''
            widgets.append(Label(f"{param['name']}{hint}", classes='param_label'))
            widgets.append(ParamInput(param, job.get(param['name'], param.get('default'))))
        buttons = Horizontal(make_button('Ausfuehren', 'run_sig', 'Operation mit diesen Werten ausfuehren',
                                         'primary'), id='form_buttons')
        widgets.append(buttons)
        return widgets

    def signature_job(self) -> dict:
        job = {k: v for k, v in (self.signature.get('job') or {}).items()}
        for field in self.query_one('#form', VerticalScroll).query(ParamInput):
            if field.value != '':
                job[field.param_name] = field.value
            else:
                job.pop(field.param_name, None)
        job['type'] = self.runner
        return job

    # --- Seiten (part_*) --------------------------------------------------------------------

    def open_page(self, name: str, params: dict):
        self.page_name = name
        self.page_params = params
        self.query_one('#page_text', Static).update(Text(f"Lade {name} ...", style='dim'))
        self.load_page(self.runner, name, params)

    def action_reload_page(self):
        if self.runner and self.page_name:
            self.open_page(self.page_name, self.page_params)

    @work(thread=True, exclusive=True, group='page')
    def load_page(self, runner: str, name: str, params: dict):
        try:
            page = self.client.page(runner, name, params)
        except NxClientError as e:
            page = {'text': '[red]' + str(e).replace('[', '\\[') + '[/]', 'actions': [], 'links': []}
        self.call_from_thread(self.show_page, runner, name, page)

    async def show_page(self, runner: str, name: str, page: dict):
        if (runner, name) != (self.runner, self.page_name):
            return
        self.query_one('#page_text', Static).update(to_text(page.get('text') or '(leer)'))
        bar = self.query_one('#page_bar', Horizontal)
        await bar.remove_children()
        await bar.mount_all([JobButton(f"#{i} {a['label'] or 'Aktion'}", a['job'], a.get('hint'))
                             for i, a in enumerate(page.get('actions', []), 1) if a.get('job')])
        self.page_links = page.get('links', [])
        links = self.query_one('#page_links', OptionList)
        links.clear_options()
        links.add_options([Option(link_prompt(i, link), id=str(i - 1)) for i, link in enumerate(self.page_links, 1)])

    def follow_link(self, link: dict):
        if link['runner'] != self.runner:
            self.select_runner(link['runner'])
        self.open_page(link['page'], link.get('params') or {})

    # --- Ausfuehren -------------------------------------------------------------------------

    def on_button_pressed(self, event: Button.Pressed):
        button = event.button
        if isinstance(button, JobButton):
            self.run_job(button.job, reload_page=True)
        elif button.id == 'run_sig' and self.signature is not None:
            self.run_job(self.signature_job())
        elif button.id == 'send_job':
            self.send_raw_job()

    def send_raw_job(self):
        try:
            job = json.loads(self.query_one('#job', TextArea).text)
        except ValueError as e:
            self.log_error(Exception('Ungueltiges JSON: ' + str(e)))
            return
        if not isinstance(job, dict):
            self.log_error(Exception('Job muss ein JSON-Objekt sein'))
            return
        self.run_job(job)

    @work(thread=True, group='jobs')
    def run_job(self, job: dict, reload_page=False):
        self.call_from_thread(self.log_line, '→ ' + json.dumps(job, ensure_ascii=False), 'dim')
        try:
            result = self.client.call(job)
        except NxClientError as e:
            self.call_from_thread(self.log_error, e)
            return
        self.call_from_thread(self.log_result, job.get('op') or job.get('type') or 'Job', result)
        if reload_page:
            self.call_from_thread(self.action_reload_page)


def main(args):
    NxRunnerTui(NxClient.from_args(args)).run()
