"""
Anmeldung an einer GitLab-Instanz per OAuth2 (Authorization Code Flow).

Die Application wird in GitLab angelegt (User Settings -> Applications oder Admin -> Applications),
als Redirect URI wird die Callback-URL aus part_index eingetragen.

Konfiguration::

    gitlab_login: nxrunner.projects.gitlab_login:GitlabLogin(url="https://gitlab.com", app_id="...", secret="...")

oder in den Args (nweb.json): gitlab.app.url, gitlab.app.app_id, gitlab.app.secret.
Fehlen Werte, zeigt part_index ein Eingabeformular.
"""
import secrets
import time
from typing import Optional
from urllib.parse import urlencode

from nwebclient import base as b
from nwebclient import runner as r
from nwebclient import util as u
from nwebclient import web as w


class GitlabLogin(r.BaseJobExecutor):
    """
    OAuth2-Anmeldung an einer GitLab-Instanz, haelt Access Token und Benutzer im Speicher
    """

    TAGS = ['gitlab', 'login']
    ARG_PREFIX = 'gitlab.app.'
    DEFAULT_URL = 'https://gitlab.com'
    DEFAULT_SCOPE = 'read_user'
    DEFAULT_HOST = 'http://localhost:7070'
    UI_PATH = '/pysys/ui'
    STATE_TTL = 600  # Sekunden, die ein begonnener Login gueltig ist
    TIMEOUT = 10

    def __init__(self, url: str = None, app_id: str = None, secret: str = None, scope: str = None,
                 callback: str = None, type: str = 'gitlab_login', args: u.Args = None):
        """
        :param url: GitLab-Instanz, sonst args gitlab.app.url, Standard https://gitlab.com
        :param app_id: Application ID der OAuth-Application, sonst args gitlab.app.app_id
        :param secret: Secret der OAuth-Application, sonst args gitlab.app.secret
        :param scope: OAuth-Scopes, durch Leerzeichen getrennt (z.B. "read_user api"), sonst args gitlab.app.scope,
                      Standard read_user; muss in der GitLab-Application angehakt sein
        :param callback: feste Callback-URL, sonst aus der aufgerufenen Adresse abgeleitet
        """
        super().__init__(type)
        self.args = args if args is not None else {}
        self.url = (url or self.arg('url') or self.DEFAULT_URL).rstrip('/')
        self.app_id = app_id or self.arg('app_id')
        self.secret = secret or self.arg('secret')
        self.scope = self.clean_scope(scope or self.arg('scope') or self.DEFAULT_SCOPE)
        self.callback = callback or self.arg('callback')
        self.states = {}  # state -> (redirect_uri, Zeitpunkt)
        self.token = None  # Antwort von /oauth/token
        self.user = None  # /api/v4/user
        self.last_error = None

    def arg(self, name: str) -> Optional[str]:
        try:
            return self.args.get(self.ARG_PREFIX + name) or None
        except Exception:
            return None

    @staticmethod
    def clean_scope(scope: str) -> str:
        """ Scopes durch Leerzeichen getrennt (Komma oder Plus werden umgewandelt) """
        return ' '.join(s for s in str(scope).replace(',', ' ').replace('+', ' ').split() if s)

    def configured(self) -> bool:
        return bool(self.url and self.app_id and self.secret)

    def missing(self) -> list:
        return [n for n in ('url', 'app_id', 'secret') if not getattr(self, n)]

    def logged_in(self) -> bool:
        return self.token is not None

    def access_token(self) -> Optional[str]:
        """ Access Token fuer andere Runner (Authorization: Bearer ...) """
        return self.token.get('access_token') if self.token else None

    # --- URLs ---------------------------------------------------------------

    def host_url(self) -> str:
        try:
            from flask import has_request_context, request
            if has_request_context():
                return request.host_url.rstrip('/')
        except ImportError:
            pass
        return self.DEFAULT_HOST

    def callback_url(self) -> str:
        """ Redirect URI, die in der GitLab-Application eingetragen werden muss """
        if self.callback:
            return self.callback
        return f'{self.host_url()}{self.UI_PATH}?type={self.type}&{self.type}=callback'

    def authorize_url(self, redirect_uri: str = None) -> str:
        """ Adresse fuer den Login bei GitLab, merkt sich state fuer den Callback """
        redirect_uri = redirect_uri or self.callback_url()
        now = time.time()
        self.states = {s: v for s, v in self.states.items() if now - v[1] < self.STATE_TTL}
        state = secrets.token_urlsafe(24)
        self.states[state] = (redirect_uri, now)
        query = urlencode({'client_id': self.app_id, 'redirect_uri': redirect_uri, 'response_type': 'code',
                           'state': state, 'scope': self.scope})
        return f'{self.url}/oauth/authorize?{query}'

    # --- OAuth --------------------------------------------------------------

    def post_token(self, data: dict) -> dict:
        import requests
        resp = requests.post(f'{self.url}/oauth/token', data=data, timeout=self.TIMEOUT)
        if resp.status_code >= 400:
            raise ValueError(f"Token-Abruf fehlgeschlagen ({resp.status_code}): {resp.text[:300]}")
        return resp.json()

    def get_user(self) -> dict:
        import requests
        resp = requests.get(f'{self.url}/api/v4/user', timeout=self.TIMEOUT,
                            headers={'Authorization': f'Bearer {self.access_token()}'})
        resp.raise_for_status()
        return resp.json()

    def handle_callback(self, code: str, state: str) -> dict:
        """ tauscht den Code gegen ein Token und laedt den Benutzer """
        entry = self.states.pop(state or '', None)
        if entry is None or time.time() - entry[1] >= self.STATE_TTL:
            raise ValueError("Unbekannter oder abgelaufener Login (state), bitte erneut anmelden")
        self.token = self.post_token({'client_id': self.app_id, 'client_secret': self.secret, 'code': code,
                                      'grant_type': 'authorization_code', 'redirect_uri': entry[0]})
        self.token['received_at'] = time.time()
        self.user = self.get_user()
        self.last_error = None
        self.info(f"Angemeldet als {self.user.get('username')}")
        return self.user

    # --- Ops ----------------------------------------------------------------

    @r.op("OAuth-Application setzen (nur im Speicher), leere Werte bleiben unveraendert",
          result={'success': 'bool', 'message': 'str'})
    def configure(self, url: str = '', app_id: str = '', secret: str = '', scope: str = ''):
        self.url = (url or self.url or self.DEFAULT_URL).rstrip('/')
        self.app_id = app_id or self.app_id
        self.secret = secret or self.secret
        self.scope = self.clean_scope(scope) or self.scope
        self.logout()
        if not self.configured():
            return self.fail("Es fehlen: " + ', '.join(self.missing()))
        return self.success("konfiguriert", ui_reload=True)

    @r.op("Login-Adresse bei GitLab erzeugen", result={'success': 'bool', 'url': 'str', 'callback': 'str'})
    def login_url(self, redirect_uri: str = ''):
        if not self.configured():
            return self.fail("Es fehlen: " + ', '.join(self.missing()))
        return self.success('ok', url=self.authorize_url(redirect_uri or None),
                            callback=redirect_uri or self.callback_url())

    @r.op("Anmeldestatus", result={'success': 'bool', 'logged_in': 'bool', 'username': 'str'})
    def status(self):
        return self.success('ok', configured=self.configured(), logged_in=self.logged_in(),
                            username=(self.user or {}).get('username', ''), url=self.url,
                            callback=self.callback_url())

    @r.op("Abmelden (Token verwerfen)", result={'success': 'bool', 'message': 'str'})
    def logout(self):
        self.token, self.user = None, None
        return self.success("abgemeldet", ui_reload=True)

    # --- HTML ---------------------------------------------------------------

    def part_index(self, p: b.Page, params={}):
        if params.get('gl_setup'):
            res = self.configure(params.get('gl_url', ''), params.get('gl_app_id', ''), params.get('gl_secret', ''),
                                 params.get('gl_scope', ''))
            if not res.get('success'):
                p.alert_error(w.s(res.get('message', '')))
        p.h2("GitLab Login")
        self.part_callback_info(p)
        if not self.configured() or params.get('edit'):
            self.part_setup(p)
            return
        self.part_status(p)
        p.pre('', id='result')

    def part_callback_info(self, p: b.Page):
        p.prop("Callback-URL", w.tag('code', w.s(self.callback_url()),
                                     title="In der GitLab-Application als Redirect URI eintragen"))
        p.prop("Instanz", w.a(w.s(self.url), self.url, hint="GitLab-Instanz öffnen"))
        p.prop("Application", w.a("Applications in GitLab", f'{self.url}/-/user_settings/applications',
                                  hint="OAuth-Application anlegen: Redirect URI = Callback-URL, "
                                       f"Scopes: {self.scope}"))

    def part_setup(self, p: b.Page):
        """ Eingabe der fehlenden Werte, POST auf dieselbe Seite (Secret nicht in der URL) """
        if self.missing():
            p.div(w.s("Es fehlen: " + ', '.join(self.missing())))
        p.start_tag('form', method='post')
        p.hidden('gl_setup', '1')
        p.form_input('gl_url', "GitLab-URL", hint="Adresse der Instanz, z.B. https://gitlab.com",
                     value=self.url or self.DEFAULT_URL)
        p.form_input('gl_app_id', "Application ID", hint="Application ID der OAuth-Application",
                     value=self.app_id or '')
        p.form_input('gl_secret', "Secret", type='password', autocomplete='off',
                     hint="Secret der OAuth-Application (leer lassen, um das gesetzte zu behalten)"
                     if self.secret else "Secret der OAuth-Application")
        p.form_input('gl_scope', "Scopes", value=self.scope,
                     hint="Durch Leerzeichen getrennt, jeder muss in der GitLab-Application angehakt sein")
        p(w.tag('input', '', type='submit', value="Speichern",
                title="Werte für diese Sitzung übernehmen (nur im Speicher, nicht in der Konfiguration)"))
        p('</form>')
        p.div(w.s(f"Dauerhaft: {self.ARG_PREFIX}url / {self.ARG_PREFIX}app_id / {self.ARG_PREFIX}secret / "
                  f"{self.ARG_PREFIX}scope in den Args oder als Konstruktor-Parameter"))

    def part_status(self, p: b.Page):
        p.prop("Application ID", w.s(self.app_id))
        p.prop("Scopes", w.s(self.scope))
        edit = w.a("Ändern", f'{self.UI_PATH}?type={self.type}&edit=1', hint="URL, Application ID und Secret ändern")
        if self.logged_in():
            self.part_user(p)
            p.div(self.action_btn(dict(title="Abmelden", type=self.type, op='logout'),
                                  hint="Token verwerfen") + ' ' + edit)
        else:
            if self.last_error:
                p.alert_error(w.s(self.last_error))
            p.div(w.a("Bei GitLab anmelden", self.authorize_url(),
                      hint=f"Weiter zu {self.url}, danach zurück über die Callback-URL") + ' ' + edit)

    def part_user(self, p: b.Page):
        user = self.user or {}
        p.prop("Angemeldet als", w.a(w.s(f"{user.get('name', '')} (@{user.get('username', '')})"),
                                     user.get('web_url', self.url), hint="Profil auf GitLab öffnen"))
        expires = self.token.get('expires_in')
        if expires:
            left = int(self.token['received_at'] + expires - time.time())
            p.prop("Token gültig", f"noch {max(left, 0) // 60} min")

    def error_text(self, error: str, description: str = '') -> str:
        """ Fehler aus der Weiterleitung von GitLab, mit Hinweis zur Behebung """
        text = f"{error}: {description}".rstrip(': ')
        if error == 'invalid_scope':
            text += (f" – angefragt: \"{self.scope}\". In GitLab bei der Application ({self.url}"
                     "/-/user_settings/applications) genau diese Scopes anhaken oder unter „Ändern“ die Scopes "
                     "der Application eintragen.")
        elif error == 'invalid_redirect_uri' or 'redirect' in description.lower():
            text += f" – als Redirect URI in GitLab eintragen: {self.callback_url()}"
        return text

    def part_callback(self, p: b.Page, params={}):
        """ Ziel der Weiterleitung von GitLab: ?code=...&state=... oder ?error=... """
        p.h2("GitLab Login")
        try:
            if params.get('error'):
                raise ValueError(self.error_text(params.get('error'), params.get('error_description', '')))
            user = self.handle_callback(params.get('code', ''), params.get('state', ''))
            p.div(w.s(f"Angemeldet als {user.get('name', '')} (@{user.get('username', '')})"))
        except Exception as e:
            self.last_error = str(e)
            p.alert_error(w.s(f"Anmeldung fehlgeschlagen: {e}"))
        p.div(w.a("Zurück", f'{self.UI_PATH}?type={self.type}', hint="Zur Übersicht des GitLab-Logins"))
