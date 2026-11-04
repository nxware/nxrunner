"""
Erzeugt die Screenshots des Design Guides (docs/design/img/*.png).

Jedes Element wird mit nwebclient.base.Page (p) bzw. nwebclient.web (w) gerendert, in die Styles
aus nxrunner/resources/ui.html eingebettet und per Edge/Chrome headless fotografiert.

  python docs/design/build.py
"""
import inspect
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

from nwebclient import base as b
from nwebclient import web as w

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
IMG = HERE / 'img'
UI_HTML = ROOT / 'nxrunner' / 'resources' / 'ui.html'

BROWSERS = [
    r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe',
    r'C:\Program Files\Google\Chrome\Application\chrome.exe',
    '/usr/bin/chromium', '/usr/bin/google-chrome', '/usr/bin/microsoft-edge',
]

# Ergaenzungen fuer Klassen, deren CSS sonst von /static kommt (grid.css, ui.css)
EXTRA_CSS = """
.nwebclient_grid table { border-collapse: collapse; width: 100%; }
.nwebclient_grid th, .nwebclient_grid td { border: 1px solid var(--border); padding: 4px 8px; text-align: left; }
.nwebclient_grid th { background: var(--bg); }
.nwebclient_grid th a { color: var(--text); text-decoration: none; }
.nwebclient_grid form.filter { display: flex; gap: 6px; margin-bottom: 8px; }
input, select, textarea { padding: 4px 8px; border: 1px solid var(--border); background: var(--panel); color: var(--text); font: inherit; }
input[type=submit] { padding: 4px 12px; cursor: pointer; }
input[type=submit]:hover { border-color: var(--accent); }
.page_form_input { margin: 4px 0; }
.prop { padding: 2px 0; }
a { color: var(--accent); }
ul.nx_menu { display: flex; flex-wrap: wrap; list-style: none; padding-left: 0; }
"""

BSNX_CSS = """
:root { --header-bg: #343a40; --header-text: #ffffff; --header-muted: #dfdfdf; --side: #212529;
  --bg: #333333; --panel: #555555; --text: #dfdfdf; --muted: #adb5bd; --border: #6c757d;
  --accent: #007bff; --accent-text: #ffffff; --ok: #28a745; --err: #dc3545; --code: #2b2b2b; }
"""


# ---------------------------------------------------------------- Elemente
# Jede Funktion rendert ein Element in p. Der Quelltext erscheint als Beispiel im Guide.

def el_typography(p):
    p.h1("Backup")
    p.h2("Quellen")
    p.p("Fließtext in 14px system-ui, Zeilenhöhe 1.5. Hinweise werden mit "
        "<span class='muted'>.muted</span> abgeschwächt.")
    p.h3("Unterüberschrift")


def el_section(p):
    with p.section(h="Fortschritt"):
        p.div("Inhalt eines Abschnitts")
        p.prop("Status", "läuft")
    with p.section(h="Ziele"):
        p.div("Abschnitte gliedern jede Runner-Seite.")


def el_expandable(p):
    with p.expandable("Erweitert", expanded=True):
        p.div("Aufgeklappter Inhalt, ohne JavaScript (&lt;details&gt;)")
    with p.expandable("Protokoll", expanded=False):
        p.div("verborgen")


def el_prop(p):
    p.prop("Version", "1.4.2")
    p.prop("Laufzeit", "3 h 12 min")
    p.prop("Ziele", ["nas", "s3"])


def el_form(p):
    p('<form method="post">')
    p.form_input('name', "Name", hint="Anzeigename der Quelle", value="Fotos")
    p.form_select('kind', "Art", {'dir': "Verzeichnis", 'mysql': "MySQL"}, value='dir',
                  hint="Art der Quelle")
    p.form_input('interval_s', "Intervall (s)", hint="0 = nur manuell", type='number', value=3600)
    p.form_elem(w.tag("input", "", type="submit", value="Speichern", title="Quelle speichern"))
    p('</form>')


def el_buttons(p):
    p.div(w.button_js("Jetzt sichern", "", hint="Startet die Sicherung sofort") + ' '
          + w.button_js("Prüfen", "", hint="Ziel auf Erreichbarkeit prüfen") + ' '
          + w.a("Fällige Sicherungen", "#", hint="Liste der fälligen Sicherungen"))


def el_alerts(p):
    p.alert_info("Info: Scheduler prüft alle 60 s")
    p.alert_success("Sicherung erfolgreich abgeschlossen")
    p.alert_warn("Ziel fast voll (92 %)")
    p.alert_error("Ziel nicht erreichbar")


def el_grid(p):
    rows = [{'name': "Fotos", 'kind': "Verzeichnis", 'size': "12,4 GB"},
            {'name': "Wiki", 'kind': "MySQL", 'size': "380 MB"},
            {'name': "Mail", 'kind': "IMAP", 'size': "2,1 GB"}]
    p.grid(rows, [w.Grid.col('name', "Name"), w.Grid.col('kind', "Art"), w.Grid.col('size', "Größe")],
           {}, search=True)


def el_pre(p):
    p.pre({'success': True, 'value': ["Fotos", "Wiki"], 'duration': 1.42})


def el_list(p):
    p.ul([w.a("Quellen", "#", hint="Alle Quellen"), w.a("Ziele", "#", hint="Alle Ziele"), "Protokoll"])
    p.hr()
    p.right(w.a("Weiter &raquo;", "#", hint="Nächste Seite"))


def el_badges(p):
    p.div('<span class="badge">laden</span> <span class="badge tag">tag</span> '
          '<span class="badge ok">ok</span> <span class="badge warn">fehler</span>')
    p.div('<span class="chip active">alle</span> <span class="chip">system</span> '
          '<span class="chip">devices</span>', style='margin-top: 8px')


def el_menu(p):
    p.div("Inhalt des Runners")
    p('<ul class="nx_menu">' + ''.join(f'<li><a href="#" title="{t}">{t}</a></li>'
                                       for t in ["StdOut", "Events", "Vars", "Help", "Run", "Sigs"]) + '</ul>')


ELEMENTS = [
    ('typography', "Typografie", el_typography),
    ('section', "p.section", el_section),
    ('expandable', "p.expandable", el_expandable),
    ('prop', "p.prop", el_prop),
    ('form', "Formulare", el_form),
    ('buttons', "Buttons & Links", el_buttons),
    ('alerts', "Meldungen", el_alerts),
    ('grid', "p.grid", el_grid),
    ('pre', "p.pre", el_pre),
    ('list', "Listen & Trenner", el_list),
    ('badges', "Badges & Chips", el_badges),
    ('menu', "Runner-Menü (nx_menu)", el_menu),
]


# ---------------------------------------------------------------- Themes

def ui_css():
    """ <style>-Block aus ui.html, damit Guide und Oberflaeche nicht auseinanderlaufen """
    m = re.search(r'<style>(.*?)</style>', UI_HTML.read_text(encoding='utf-8'), re.S)
    return m.group(1) if m else ''


def theme_css(theme='dark'):
    """ Nur Darkmode: Dunkel-Schema unabhaengig von prefers-color-scheme; bsnx: Farben von bsnx.net/4.0 """
    css = ui_css().replace('(prefers-color-scheme: dark)', '(min-width: 0)')
    if theme == 'bsnx':
        css += BSNX_CSS
    return css + EXTRA_CSS


def doc(body, theme='dark', body_style=''):
    return ('<!doctype html><html lang="de"><head><meta charset="utf-8"><style>' + theme_css(theme)
            + 'body { padding: 0; ' + body_style + ' }</style></head><body>' + body + '</body></html>')


def element_html(fn):
    p = b.Page()
    fn(p)
    body = '<div style="padding: 16px"><section class="card" id="content">' + str(p) + '</section></div>'
    return doc(body)


# ---------------------------------------------------------------- Layouts

def shell_content():
    p = b.Page()
    el_section(p)
    el_alerts(p)
    el_menu(p)
    return str(p)


def nxrunner_shell():
    runners = ''.join(f'<li class="{c}"><div><div>{n}</div><div class="cls">{k}</div></div></li>'
                      for n, k, c in [("backup", "nxrunner.system.backup:Backup", "active"),
                                      ("docker", "nxrunner.dockerproxy:DockerProxy", ""),
                                      ("esphome", "nxrunner.devices.esphome:EspHome", "")])
    runners += '<h3>Nicht geladen</h3><li><div><div>mqtt</div><div class="cls">nxrunner.apis.mqtt:Mqtt</div></div>' \
               '<span class="badge">laden</span></li>'
    nav = ''.join(f'<a href="#" title="{t}">{t}</a>' for t in ["Registry", "Jobs", "Docs", "Terminal"])
    return ('<header><h1>NxRunner</h1><span class="stats">4 Runner geladen, 21 verfügbar</span>'
            f'<nav id="nav">{nav}</nav></header><main><aside class="card">'
            '<input type="search" placeholder="Runner suchen&hellip;">'
            f'<ul class="list">{runners}</ul></aside>'
            f'<section class="card" id="content"><h1>Backup</h1>{shell_content()}</section></main>')


BSNX_SHELL_CSS = """
.bs-nav { display: flex; align-items: center; gap: 12px; padding: 8px 16px; background: var(--header-bg); color: #fff; }
.bs-nav b { font-size: 18px; }
.bs-nav .r { margin-left: auto; color: var(--header-muted); }
.bs-wrap { display: flex; min-height: 640px; }
.bs-side { width: 200px; background: var(--side); padding: 8px 0; }
.bs-side a { display: block; padding: 10px 16px; color: rgba(255,255,255,.5); text-decoration: none; }
.bs-side a.active, .bs-side a:hover { color: #fff; }
.bs-main { flex: 1; padding: 16px; }
.bs-main .card { background: var(--panel); color: #f6f6f6; }
.bs-foot { background: #e9ecef; color: #6c757d; text-align: center; padding: 16px; font-size: 12px; }
"""


def bsnx_shell():
    side = ''.join(f'<a href="#" class="{c}" title="{t}">{t}</a>'
                   for t, c in [("Login", ""), ("Docs", ""), ("Images", ""), ("Users", ""), ("Runner", "active")])
    return (f'<style>{BSNX_SHELL_CSS}</style><div class="bs-nav"><b>nweb</b> &#9776;<span class="r">&#10140;</span></div>'
            f'<div class="bs-wrap"><div class="bs-side">{side}</div><div class="bs-main">'
            f'<section class="card" id="content"><h1>Backup</h1>{shell_content()}</section></div></div>'
            '<div class="bs-foot">nweb</div>')


LAYOUTS = [
    ('layout_nxrunner', "NxRunner", lambda: doc(nxrunner_shell())),
    ('layout_bsnx', "bsnx.net 4.0", lambda: doc(bsnx_shell(), 'bsnx')),
]


# ---------------------------------------------------------------- Screenshots

def find_browser():
    for path in BROWSERS:
        if os.path.isfile(path):
            return path
    return shutil.which('msedge') or shutil.which('chromium') or shutil.which('google-chrome')


def screenshot(browser, html, png, width, height, tmp):
    src = Path(tmp) / (png.stem + '.html')
    src.write_text(html, encoding='utf-8')
    subprocess.run([browser, '--headless=new', '--disable-gpu', '--hide-scrollbars',
                    f'--user-data-dir={Path(tmp) / "profile"}', f'--window-size={width},{height}',
                    f'--screenshot={png}', src.as_uri()],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
    crop(png)


def crop(png, pad=16):
    """ Leerraum unten abschneiden (Farbe der linken unteren Ecke) """
    from PIL import Image, ImageChops
    with Image.open(png) as img:
        img = img.convert('RGB')
        bg = Image.new('RGB', img.size, img.getpixel((0, img.height - 1)))
        box = ImageChops.difference(img, bg).getbbox()
        if box:
            img = img.crop((0, 0, img.width, min(img.height, box[3] + pad)))
        img.save(png)


# ---------------------------------------------------------------- Markdown

def example_source(fn):
    lines = inspect.getsource(fn).splitlines()[1:]
    return textwrap.dedent('\n'.join(lines)).strip()


def elements_md():
    res = ''
    for key, title, fn in ELEMENTS:
        res += f'### {title}\n\n```python\n{example_source(fn)}\n```\n\n'
        res += f'![{title}](img/{key}.png)\n\n'
    return res


def swatch(hex_code):
    """ Kleines Farbfeld img/colors/<hex>.png, Rand in der Textfarbe damit es auf jedem Hintergrund sichtbar ist """
    from PIL import Image, ImageDraw
    path = IMG / 'colors' / (hex_code.lower() + '.png')
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        img = Image.new('RGB', (14, 14), '#' + hex_code)
        ImageDraw.Draw(img).rectangle((0, 0, 13, 13), outline='#808080')
        img.save(path)
    return path.relative_to(HERE).as_posix()


def with_swatches(md):
    """ Vor jeden Farbcode `#rgb` bzw. `#rrggbb` ein Farbfeld setzen """
    return re.sub(r'`#([0-9a-fA-F]{6}|[0-9a-fA-F]{3})`',
                  lambda m: f'<img src="{swatch(m.group(1))}" width="14" height="14" alt="#{m.group(1)}"> {m.group(0)}',
                  md)


def write_md():
    template = (HERE / 'DesignGuide.template.md').read_text(encoding='utf-8')
    md = with_swatches(template.replace('{{ELEMENTS}}', elements_md()))
    (HERE / 'DesignGuide.md').write_text(md, encoding='utf-8', newline='\n')


def main():
    browser = find_browser()
    if browser is None:
        print("Kein Edge/Chrome gefunden", file=sys.stderr)
        return 1
    IMG.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        for key, title, fn in ELEMENTS:
            screenshot(browser, element_html(fn), IMG / f'{key}.png', 760, 1000, tmp)
        for key, title, render in LAYOUTS:
            screenshot(browser, render(), IMG / f'{key}.png', 1280, 1000, tmp)
    write_md()
    print("DesignGuide.md und", len(list(IMG.glob('*.png'))), "Screenshots erzeugt")
    return 0


if __name__ == '__main__':
    sys.exit(main())
