"""
  Client fuer den REST-Service eines laufenden nxrunner (python -m nxrunner serv).

  Jobs werden als JSON per POST an / geschickt, Antwort ist das Job-Ergebnis (meist JSON).
  Mit --web_secured am Server ist HTTP Basic Auth noetig (user/password).
"""
import json
import os

import requests

# 127.0.0.1 statt localhost: unter Windows sonst ~2s Verzoegerung durch IPv6-Versuch (Server lauscht auf 0.0.0.0)
DEFAULT_URL = 'http://127.0.0.1:7070'


class NxClientError(Exception):
    pass


class NxClient:

    def __init__(self, url=None, user=None, password=None, timeout=120):
        self.url = (url or os.environ.get('NXRUNNER_URL') or DEFAULT_URL).rstrip('/')
        user = user or os.environ.get('NXRUNNER_USER')
        password = password or os.environ.get('NXRUNNER_PASSWORD')
        self.auth = (user or 'admin', password) if password else None
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = self.auth

    @classmethod
    def from_args(cls, args):
        """
          aus nwebclient.util.Args: --url oder --host/--port, --user, --password
          Nur von der Kommandozeile (getValue): args.get liest auch nweb.json/nweb.yml,
          dort ist z.B. url die Adresse des nweb-Servers und nicht die des nxrunner.
        """
        url = args.getValue('url')
        host, port = args.getValue('host'), args.getValue('port')
        if url is None and (host or port):
            url = f"http://{host or '127.0.0.1'}:{port or 7070}"
        return cls(url, args.getValue('user'), args.getValue('password'))

    def call(self, job: dict):
        """ fuehrt einen Job aus, Ergebnis als dict (JSON) oder Text """
        try:
            response = self.session.post(self.url + '/', json=job, timeout=self.timeout)
        except requests.RequestException as e:
            raise NxClientError(f"Keine Verbindung zu {self.url}: {e}") from e
        if response.status_code == 401:
            raise NxClientError("Login erforderlich (--user/--password oder NXRUNNER_USER/NXRUNNER_PASSWORD)")
        try:
            return response.json()
        except ValueError:
            return response.text

    def get_text(self, path: str) -> str:
        response = self.session.get(self.url + path, timeout=self.timeout)
        response.raise_for_status()
        return response.text

    def ping(self) -> str:
        """ Name des Runners (/nxname), wirft NxClientError ohne Verbindung """
        try:
            return self.get_text('/nxname')
        except requests.RequestException as e:
            raise NxClientError(f"Keine Verbindung zu {self.url}: {e}") from e

    def runners(self) -> list:
        """ [{name, cls, loaded}] aller geladenen und konfigurierten Runner """
        result = self.call({'op': 'runners'})
        if isinstance(result, dict) and 'runners' in result:
            return result['runners']
        # aeltere Server: nur die Namen
        result = self.call({'type': 'list_runners'})
        names = result.get('names', []) if isinstance(result, dict) else []
        return [{'name': n, 'cls': '', 'loaded': False} for n in names]

    def signatures(self, runner_type: str) -> list:
        result = self.call({'type': runner_type, 'op': 'get_signatures'})
        if isinstance(result, dict) and isinstance(result.get('signatures'), list):
            return result['signatures']
        message = result.get('message', '') if isinstance(result, dict) else str(result)
        raise NxClientError(f"Keine Signaturen fuer {runner_type}: {message}")


    def parts(self, runner_type: str) -> list:
        """ Namen der part_*-Seiten eines Runners """
        result = self.call({'op': 'parts', 'runner': runner_type})
        if isinstance(result, dict) and isinstance(result.get('parts'), list):
            return result['parts']
        message = result.get('message', '') if isinstance(result, dict) else str(result)
        raise NxClientError(f"Keine Seiten fuer {runner_type}: {message}")

    def page(self, runner_type: str, page: str, params: dict = None) -> dict:
        """ part_<page> als TerminalPage: {text, actions, links} """
        result = self.call({**(params or {}), 'op': 'terminal_page', 'runner': runner_type, 'page': page})
        if isinstance(result, dict) and 'text' in result:
            return result
        message = result.get('message', '') if isinstance(result, dict) else str(result)
        raise NxClientError(f"Seite {runner_type}/{page}: {message}")


def format_result(result) -> str:
    if isinstance(result, (dict, list)):
        return json.dumps(result, indent=2, ensure_ascii=False, default=str)
    return str(result)
