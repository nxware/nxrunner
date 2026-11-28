"""
  TerminalPage: base.Page, die part_*-Seiten der Runner als Terminal-Text (Rich-Markup) ausgibt.

  Die Runner schreiben wie gewohnt HTML in die Seite (p.h1, p.prop, w.button_js, ...), beim Rendern wird das HTML
  in Text umgesetzt. Buttons mit exec_job(...) werden zu Aktionen (Jobs), Links auf Runner-Seiten zu Links.

    p = TerminalPage(owner=runner)
    runner.part_index(p, params)
    p.render() -> {'text': ..., 'actions': [{label, hint, job}], 'links': [{title, url, runner, page, params}]}
"""
import inspect
import json
import re
import urllib.parse
from html.parser import HTMLParser

from nwebclient import base

SKIP_TAGS = {'script', 'style', 'head', 'template', 'noscript'}
BLOCK_TAGS = {'p', 'div', 'section', 'article', 'header', 'footer', 'nav', 'form', 'fieldset', 'main',
              'aside', 'figure', 'details', 'summary', 'ul', 'ol', 'dl', 'dt', 'dd', 'blockquote'}
INLINE_STYLES = {'b': 'b', 'strong': 'b', 'i': 'i', 'em': 'i', 'code': 'cyan', 'kbd': 'cyan', 'small': 'dim',
                 'mark': 'reverse'}
HEADINGS = {'h1': 'b reverse', 'h2': 'b u', 'h3': 'b', 'h4': 'b', 'h5': 'b', 'h6': 'b'}
ALERTS = (('900', 'red', '✖ '), ('090', 'green', '✔ '), ('990', 'yellow', '⚠ '), ('a60', 'yellow', '⚠ '),
          ('960', 'yellow', '⚠ '), ('009', 'blue', 'ℹ '), ('069', 'blue', 'ℹ '))
BUTTON_JS_RE = re.compile(r'getElementById\("([^"]+)"\)\.addEventListener\("click"')
EXEC_JOB_RE = re.compile(r'exec_job(_p)?\(')


def escape(text: str) -> str:
    """ Text fuer Rich-Markup maskieren """
    return text.replace('\\', '\\\\').replace('[', '\\[')


def parse_runner_link(url: str):
    """ ?type=x&x=page&a=1 oder /pysys/dispatcher?... -> (runner, page, params) oder None """
    query = urllib.parse.urlparse(url).query if '?' in url else ''
    params = dict(urllib.parse.parse_qsl(query))
    runner = params.get('type')
    if not runner:
        return None
    page = params.pop(runner, 'index')
    params.pop('type', None)
    return runner, page, params


def find_job(js: str, start: int):
    """ Job-Objekt aus dem ersten exec_job({...}) ab start, (job, parametrisch) oder (None, False) """
    m = EXEC_JOB_RE.search(js, start)
    if m is None:
        return None, False
    try:
        job, _ = json.JSONDecoder().raw_decode(js, m.end())
    except ValueError:
        return None, False
    return (job, m.group(1) is not None) if isinstance(job, dict) else (None, False)


def extract_button_jobs(scripts: str) -> dict:
    """ {button_id: (job, parametrisch)} aus den Scripten von web.button_js """
    res = {}
    for m in BUTTON_JS_RE.finditer(scripts):
        job = find_job(scripts, m.end())
        if job[0] is not None:
            res[m.group(1)] = job
    return res


class HtmlToTerminal(HTMLParser):

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out = []
        self.skip = 0
        self.pre = 0
        self.scripts = []
        self.styles = []  # offene Markup-Tags (Rich), je HTML-Element ein Eintrag oder None
        self.list_depth = 0
        self.table = None  # Liste von Zeilen, Zeile = Liste von Zellen
        self.cell = None
        self.button = None
        self.link = None
        self.actions = []
        self.links = []

    # --- Ausgabe ----------------------------------------------------------------------------

    def emit(self, text: str):
        if self.link is not None:
            self.link['title'] += text
        if self.button is not None:
            self.button['label'] += text
        elif self.cell is not None:
            self.cell.append(text)
        else:
            self.out.append(text)

    def text(self) -> str:
        return ''.join(self.out)

    def newline(self, count=1):
        if self.cell is not None or self.button is not None:
            return
        current = self.text()
        if current == '':
            return
        missing = count - (len(current) - len(current.rstrip('\n')))
        if missing > 0:
            self.out.append('\n' * missing)

    # --- Parser -----------------------------------------------------------------------------

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag in SKIP_TAGS:
            self.skip += 1
            return
        if self.skip:
            return
        handler = getattr(self, 'start_' + tag, None)
        if handler is not None:
            handler(attrs)
        elif tag in HEADINGS:
            self.newline(2)
            self.open_style(HEADINGS[tag])
        elif tag in INLINE_STYLES:
            self.open_style(INLINE_STYLES[tag])
        elif tag in BLOCK_TAGS:
            self.newline()
            self.open_alert(attrs) if tag == 'div' else None

    def handle_endtag(self, tag):
        if tag in SKIP_TAGS:
            self.skip = max(0, self.skip - 1)
            return
        if self.skip:
            return
        handler = getattr(self, 'end_' + tag, None)
        if handler is not None:
            handler()
        elif tag in HEADINGS:
            self.close_style()
            self.newline(2)
        elif tag in INLINE_STYLES:
            self.close_style()
        elif tag in BLOCK_TAGS:
            if tag == 'div':
                self.close_style()
            self.newline()

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in ('br', 'hr', 'input', 'img'):
            self.handle_endtag(tag)

    def handle_data(self, data):
        if self.skip:
            self.scripts.append(data)
            return
        if self.pre:
            self.emit(escape(data))
            return
        text = re.sub(r'\s+', ' ', data)
        if text.strip() == '' and (self.text().endswith('\n') or self.text() == ''):
            return
        self.emit(escape(text))

    # --- Markup -----------------------------------------------------------------------------

    def open_style(self, style):
        self.styles.append(style)
        if style:
            self.emit('[' + style + ']')

    def close_style(self):
        style = self.styles.pop() if self.styles else None
        if style:
            self.emit('[/]')

    def open_alert(self, attrs):
        """ Alerts aus nwebclient.web (border-left mit Farbe) farbig mit Symbol """
        css = (attrs.get('style') or '') + ' ' + (attrs.get('class') or '')
        if 'border-left' not in css and 'alert' not in css:
            self.styles.append(None)
            return
        color, symbol = next(((c, s) for key, c, s in ALERTS if '#' + key in css), ('blue', 'ℹ '))
        self.open_style(color)
        self.emit(symbol)

    # --- einzelne Tags ----------------------------------------------------------------------

    def start_br(self, attrs):
        self.emit('\n') if self.cell is None else self.emit(' ')

    def start_hr(self, attrs):
        self.newline()
        self.emit('[dim]' + '─' * 40 + '[/]')
        self.newline()

    def start_pre(self, attrs):
        self.newline()
        self.pre += 1
        self.open_style('cyan')

    def end_pre(self):
        self.close_style()
        self.pre = max(0, self.pre - 1)
        self.newline()

    def start_li(self, attrs):
        self.newline()
        self.emit('  ' * max(0, self.list_depth - 1) + '  • ')

    def start_ul(self, attrs):
        self.list_depth += 1
        self.newline()

    def end_ul(self):
        self.list_depth = max(0, self.list_depth - 1)
        self.newline()

    start_ol, end_ol = start_ul, end_ul

    def start_img(self, attrs):
        self.emit('[dim]\\[Bild ' + escape(attrs.get('alt') or attrs.get('title') or '') + '][/]')

    def start_input(self, attrs):
        if (attrs.get('type') or 'text') in ('hidden', 'submit', 'button'):
            return
        name = attrs.get('name') or attrs.get('id') or 'input'
        self.emit('[dim]\\[' + escape(name) + '=' + escape(attrs.get('value') or '') + '][/]')

    def start_textarea(self, attrs):
        self.start_input({'name': attrs.get('name') or attrs.get('id'), 'type': 'text'})
        self.skip += 1

    def end_textarea(self):
        self.skip = max(0, self.skip - 1)

    def start_select(self, attrs):
        self.start_input({'name': attrs.get('name') or attrs.get('id'), 'type': 'text'})
        self.skip += 1

    end_select = end_textarea

    # --- Links und Buttons ------------------------------------------------------------------

    def start_a(self, attrs):
        self.link = {'title': '', 'url': attrs.get('href') or '', 'hint': attrs.get('title') or ''}
        self.emit('[u]')

    def end_a(self):
        if self.link is None:
            return
        link, self.link = self.link, None
        self.emit('[/]')
        link['title'] = plain_text(link['title']).strip()
        target = parse_runner_link(link['url'])
        if target is not None:
            link.update(runner=target[0], page=target[1], params=target[2])
            self.links.append(link)
            self.emit('[dim]→' + str(len(self.links)) + '[/]')

    def start_button(self, attrs):
        self.button = {'id': attrs.get('id'), 'label': '', 'hint': attrs.get('title') or ''}

    def end_button(self):
        if self.button is None:
            return
        button, self.button = self.button, None
        button['label'] = re.sub(r'\s+', ' ', button['label']).strip()
        self.actions.append(button)
        self.emit('[reverse] ' + button['label'] + ' [/][dim]#' + str(len(self.actions)) + '[/]')

    # --- Tabellen ---------------------------------------------------------------------------

    def start_table(self, attrs):
        self.newline()
        self.table = []

    def start_tr(self, attrs):
        if self.table is not None:
            self.table.append([])

    def start_td(self, attrs):
        if self.table is not None:
            if not self.table:
                self.table.append([])
            self.cell = []

    start_th = start_td

    def end_td(self):
        if self.table is not None and self.cell is not None:
            self.table[-1].append(re.sub(r'\s+', ' ', ''.join(self.cell)).strip())
        self.cell = None

    end_th = end_td

    def end_table(self):
        rows = [row for row in (self.table or []) if row]
        self.table = None
        if rows:
            self.out.append(render_table(rows))
        self.newline()

    # --- Ergebnis ---------------------------------------------------------------------------

    def result(self) -> dict:
        jobs = extract_button_jobs(''.join(self.scripts))
        actions = []
        for button in self.actions:
            job, parametric = jobs.get(button.get('id'), (None, False))
            actions.append({'label': button['label'], 'hint': button['hint'], 'job': job, 'parametric': parametric})
        text = re.sub(r'\n{3,}', '\n\n', self.text()).strip('\n')
        return {'text': text, 'actions': actions, 'links': self.links}


def plain_text(text: str) -> str:
    """ Rich-Markup entfernen und Maskierung aufheben """
    plain = re.sub(r'(?<!\\)\[/?[a-z ]*\]', '', text)
    return plain.replace('\\[', '[').replace('\\\\', '\\')


def markup_len(text: str) -> int:
    """ sichtbare Laenge von Rich-Markup """
    return len(plain_text(text))


def render_table(rows: list) -> str:
    cols = max(len(r) for r in rows)
    widths = [min(40, max((markup_len(r[i]) for r in rows if i < len(r)), default=0)) for i in range(cols)]
    lines = []
    for n, row in enumerate(rows):
        cells = [cell + ' ' * max(0, widths[i] - markup_len(cell)) for i, cell in enumerate(row)]
        lines.append('  '.join(cells).rstrip())
        if n == 0 and len(rows) > 1:
            lines.append('[dim]' + '  '.join('─' * w for w in widths) + '[/]')
    return '\n'.join(lines) + '\n'


class TerminalPage(base.Page):
    """ base.Page fuer das Terminal: sammelt HTML wie eine normale Seite, render() liefert Text """

    def script(self, js):
        return self  # JS des Owners wird im Terminal nicht gebraucht

    def style(self, s):
        return self

    def render(self) -> dict:
        parser = HtmlToTerminal()
        parser.feed(self.s)
        parser.close()
        return parser.result()

    def text(self) -> str:
        return self.render()['text']

    def nxui(self, params={}):
        return self.text()


def render_part(runner, page_name: str, params: dict) -> dict:
    """ part_<page_name> des Runners in eine TerminalPage rendern """
    part = getattr(runner, 'part_' + page_name, None)
    if not callable(part):
        return {'success': False, 'message': f'Keine Seite part_{page_name}'}
    p = TerminalPage(owner=runner)
    missing = getattr(runner, 'part_missing_modules', None)
    if callable(missing):
        missing(p)
    if part_arity(part) == 1:
        part(p)
    else:
        part(p, params)
    return {'success': True, 'page': page_name, **p.render()}


def part_arity(part):
    """
      1: part(p), 2: part(p, params), None: keine Seite (Hilfs-part mit anderen Pflicht-Parametern,
      z.B. part_breadcrumb(p, path))
    """
    try:
        sig = inspect.signature(part)
    except (TypeError, ValueError):
        return 2
    params = [x for x in sig.parameters.values() if x.kind in (x.POSITIONAL_ONLY, x.POSITIONAL_OR_KEYWORD)]
    if any(x.kind == x.VAR_POSITIONAL for x in sig.parameters.values()):
        return 2
    if len(params) == 1:
        return 1
    if len(params) >= 2 and (params[1].name == 'params' or params[1].default is not params[1].empty) \
            and all(x.default is not x.empty for x in params[2:]):
        return 2
    return None


def part_names(runner) -> list:
    """ Namen der part_*-Seiten (ohne missing_modules, das render_part immer voranstellt) """
    res = []
    for n in dir(type(runner)):
        part = getattr(runner, n, None) if n.startswith('part_') and n != 'part_missing_modules' else None
        if callable(part) and part_arity(part) is not None:
            res.append(n[5:])
    return sorted(res)
