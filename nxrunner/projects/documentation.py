"""
Anbindung von nxdoc (n-py/modules/nxdoc, Python Documentation Viewer) an nxrunner.

nxdoc ist ein Flask-Blueprint mit den Routen /documentation/... . nxrunner haengt ihn in seine Web-App ein
(NxRunner.setupRestApp -> register). Ohne Web-App (z.B. ein anderer Dispatcher) startet start_server einen
eigenen Flask-Server nur fuer nxdoc.

nxdoc zeigt nur Pakete aus nweb_cfg['nxdoc']['prefixes']. add_package traegt ein Paket dort ein, mit seinem
Quellverzeichnis in nweb_cfg['repos'] (README.md und docs/ des Pakets).
"""
import importlib.util
import threading
import urllib.parse


STATIC_URL = 'https://bsnx.net/static/'

# Basis-URL der nxdoc-Seiten, None solange nxdoc nirgends eingehaengt ist
base_url = None
server = None
server_error = ''


def installed() -> bool:
    """ True wenn das Modul nxdoc installiert ist """
    try:
        return importlib.util.find_spec('nxdoc') is not None
    except (ImportError, ValueError):
        return False


def blueprint():
    from nxdoc import app
    return app


def config() -> dict:
    """ nweb_cfg von nxdoc, wird angelegt wenn nxdoc nicht schon von n-py konfiguriert wurde """
    bp = blueprint()
    cfg = getattr(bp, 'nweb_cfg', None)
    if cfg is None:
        cfg = {'NPY_PUBLIC_URL': '/'}
        bp.nweb_cfg = cfg
    cfg.setdefault('NPY_PUBLIC_URL', '/')
    cfg.setdefault('nxdoc', {}).setdefault('prefixes', [])
    cfg.setdefault('repos', {})
    return cfg


def add_package(name: str, path: str = None):
    """ Paket fuer nxdoc freischalten (nxdoc.prefixes), path: Quellverzeichnis (repos) """
    cfg = config()
    if name not in cfg['nxdoc']['prefixes']:
        cfg['nxdoc']['prefixes'].append(name)
    if path is not None:
        cfg['repos'].setdefault(name, str(path))


def add_static_redirect(app):
    """ /static/... (CSS und JS der nxui-Seiten) nach bsnx.net umleiten, falls die App sie nicht selbst liefert """
    from flask import redirect
    if any(r.rule.startswith('/static/') for r in app.url_map.iter_rules()):
        return
    app.add_url_rule('/static/<path:path>', 'nxdoc_static',
                     view_func=lambda path: redirect(STATIC_URL + urllib.parse.quote(path)))


def register(app, url='/') -> bool:
    """ nxdoc in eine Flask-App einhaengen, url: Basis-URL der App (ohne documentation/) """
    global base_url
    if not installed():
        return False
    config()
    if 'nxdoc' not in app.blueprints:
        app.register_blueprint(blueprint())
        add_static_redirect(app)
    base_url = url
    return True


def start_server(host='127.0.0.1', port=7012) -> bool:
    """ eigener Flask-Server nur fuer nxdoc, wenn keine Web-App vorhanden ist """
    global server, server_error, base_url
    if server is not None:
        return True
    try:
        from flask import Flask
        from werkzeug.serving import make_server
        app = Flask('nxrunner.documentation', static_folder=None)
        register(app)
        server = make_server(host, int(port), app, threaded=True)
    except Exception as e:
        server_error = f'{type(e).__name__}: {e}'
        return False
    server_error = ''
    threading.Thread(target=server.serve_forever, daemon=True, name='nxdoc-server').start()
    base_url = f'http://{"127.0.0.1" if host == "0.0.0.0" else host}:{int(port)}/'
    return True


def url(pkg: str):
    """ URL der nxdoc-Seite eines Pakets, None wenn nxdoc nicht eingehaengt ist """
    if base_url is None:
        return None
    return base_url + 'documentation/' + urllib.parse.quote(pkg, safe='._-')
