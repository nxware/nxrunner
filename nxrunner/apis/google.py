"""
Google Kalender (Google Calendar API v3) als Runner, mit den Farben der Termine.

Anmeldung (credentials = JSON aus der Google Cloud Console):
  - Dienstkonto ("type": "service_account"): der Kalender muss fuer die E-Mail des Dienstkontos freigegeben sein,
    calendar_id dann die ID des Kalenders (nicht 'primary')
  - OAuth-Client (Desktop-App): Anmeldung ueber die Seite des Runners (Link oeffnen, zustimmen, die Adresse,
    auf die Google weiterleitet, einfuegen), das Token wird in token gespeichert und automatisch erneuert

Farben: Termine haben eine colorId 1-11 (Palette von colors().get(), Namen wie im Google Kalender),
ohne colorId gilt die Farbe des Kalenders.

    Config (runners):
        google_calendar: nxrunner.apis.google:CalendarAPI(credentials="~/.config/nxrunner/google_credentials.json")
"""
import datetime
import html
import json
import os
import threading

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import web as w
from nwebclient import dev as d

SCOPES = ['https://www.googleapis.com/auth/calendar']

# Namen der Terminfarben (colorId) wie in der Oberflaeche des Google Kalenders
COLOR_NAMES = {
    '1': 'Lavendel', '2': 'Salbei', '3': 'Weintraube', '4': 'Flamingo', '5': 'Banane', '6': 'Mandarine',
    '7': 'Pfau', '8': 'Graphit', '9': 'Heidelbeere', '10': 'Basilikum', '11': 'Tomate'
}


class CalendarAPI(r.BaseJobExecutor):
    """
      Termine eines Google Kalenders anzeigen, anlegen, loeschen und einfaerben.
      op=calendars                   Kalender des Kontos (id, summary, Farbe)
      op=colors                      Terminfarben {colorId: {name, background, foreground}}
      op=events [calendar_id] [days=14] [q] [max=50]   kommende Termine mit Farbe
      op=create summary start [end] [color_id] [description] [location] [calendar_id]
                                     start/end als 2026-10-23 (ganztaegig) oder 2026-10-23T10:00
      op=set_color event_id [color_id] [calendar_id]   Farbe setzen, leer = Farbe des Kalenders
      op=delete event_id [calendar_id]
      op=set_calendar calendar_id    Kalender fuer die Seite und als Standard
      op=save_credentials json=...   Zugangsdaten (JSON aus der Google Cloud Console) speichern
      op=auth_url [new] | op=auth_code code   OAuth-Anmeldung (code: Code oder die ganze Weiterleitungs-Adresse)
      Ohne Zugangsdaten zeigt die Seite eine Anleitung zum Einrichten, ohne Anmeldung direkt den Anmelde-Link.
    """
    MODULES = ['google-api-python-client', 'google-auth-oauthlib']
    TAGS = ['api', 'calendar']

    def __init__(self, type='google_calendar', credentials='~/.config/nxrunner/google_credentials.json',
                 token='~/.config/nxrunner/google_token.json', calendar_id='primary', days=14):
        super().__init__(type)
        self.credentials_file = os.path.expanduser(credentials)
        self.token_file = os.path.expanduser(token)
        self.calendar_id = calendar_id
        self.days = int(days)
        self.lock = threading.Lock()
        self._service = None
        self._colors = None
        self._time_zones = {}
        self.flow = None  # laufende OAuth-Anmeldung
        self.auth_link = None
        self.define_vars('calendar_id', 'days')
        self.define_sig(d.PStr('op', 'calendars')).r(success='bool', message='str', value='list')
        self.define_sig(d.PStr('op', 'colors')).r(success='bool', message='str', value='dict')
        self.define_sig(d.PStr('op', 'events'), d.PStr('calendar_id', calendar_id), d.PInt('days', self.days),
                        d.PStr('q', ''), d.PInt('max', 50)).r(success='bool', message='str', value='list')
        self.define_sig(d.PStr('op', 'create'), d.PStr('summary', ''), d.PStr('start', ''), d.PStr('end', ''),
                        d.PStr('color_id', ''), d.PStr('description', ''), d.PStr('location', ''),
                        d.PStr('calendar_id', '')).r(success='bool', message='str', value='dict')
        self.define_sig(d.PStr('op', 'set_color'), d.PStr('event_id', ''), d.PStr('color_id', ''),
                        d.PStr('calendar_id', '')).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'delete'), d.PStr('event_id', ''), d.PStr('calendar_id', '')).r(success='bool', message='str')
        self.define_sig(d.PStr('op', 'set_calendar'), d.PStr('calendar_id', '')).r(success='bool', message='str')

    # --- Anmeldung ----------------------------------------------------------------------------

    def credentials_info(self) -> dict:
        if not os.path.isfile(self.credentials_file):
            raise RuntimeError(f'Zugangsdaten fehlen: {self.credentials_file} (JSON aus der Google Cloud Console)')
        with open(self.credentials_file, encoding='utf-8') as f:
            return json.load(f)

    def is_service_account(self) -> bool:
        return self.credentials_info().get('type') == 'service_account'

    def load_credentials(self):
        """ gueltige Credentials oder None, wenn noch eine OAuth-Anmeldung noetig ist """
        info = self.credentials_info()
        if info.get('type') == 'service_account':
            from google.oauth2 import service_account
            return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        from google.oauth2.credentials import Credentials
        if not os.path.isfile(self.token_file):
            return None
        creds = Credentials.from_authorized_user_file(self.token_file, SCOPES)
        if creds.valid:
            return creds
        if creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request
            creds.refresh(Request())
            self.save_token(creds)
            return creds
        return None

    def save_token(self, creds):
        os.makedirs(os.path.dirname(self.token_file) or '.', exist_ok=True)
        with open(self.token_file, 'w', encoding='utf-8') as f:
            f.write(creds.to_json())
        os.chmod(self.token_file, 0o600)

    def is_authorized(self) -> bool:
        try:
            return self.load_credentials() is not None
        except Exception:
            return False

    def service(self):
        with self.lock:
            if self._service is None:
                creds = self.load_credentials()
                if creds is None:
                    raise RuntimeError('Nicht angemeldet: Anmeldung auf der Seite des Runners (op=auth_url)')
                from googleapiclient.discovery import build
                self._service = build('calendar', 'v3', credentials=creds, cache_discovery=False)
            return self._service

    def execute_save_credentials(self, data):
        """ Zugangsdaten (JSON aus der Google Cloud Console: OAuth-Client oder Dienstkonto) speichern """
        try:
            info = json.loads(str(data.get('json', '')))
        except ValueError as e:
            return self.fail(f'Kein gueltiges JSON: {e}')
        if not isinstance(info, dict) or not (info.get('type') == 'service_account' or 'installed' in info or 'web' in info):
            return self.fail('Erwartet wird das JSON eines OAuth-Clients ("installed") oder eines Dienstkontos')
        os.makedirs(os.path.dirname(self.credentials_file) or '.', exist_ok=True)
        with open(self.credentials_file, 'w', encoding='utf-8') as f:
            json.dump(info, f, indent=2)
        os.chmod(self.credentials_file, 0o600)
        with self.lock:
            self._service = None
        self.flow, self.auth_link = None, None
        return self.success('Zugangsdaten gespeichert: ' + self.credentials_file, ui_reload=True)

    def execute_auth_url(self, data={}):
        """ Anmelde-Link fuer einen OAuth-Client (Desktop-App), Google leitet danach auf http://localhost weiter """
        if self.is_service_account():
            return self.fail('Dienstkonto, keine Anmeldung noetig')
        # laufende Anmeldung wiederverwenden: ein neuer Link wuerde den state der bereits geoeffneten ungueltig machen
        if self.flow is None or 'new' in data:
            from google_auth_oauthlib.flow import Flow
            self.flow = Flow.from_client_config(self.credentials_info(), SCOPES, redirect_uri='http://localhost')
            self.auth_link, _ = self.flow.authorization_url(access_type='offline', prompt='consent')
        return self.success('Link oeffnen, zustimmen und die Adresse, auf die weitergeleitet wird, einfuegen',
                            value=self.auth_link)

    def execute_auth_code(self, data):
        code = str(data.get('code', '')).strip()
        if self.flow is None:
            return self.fail('Zuerst den Anmelde-Link erzeugen (op=auth_url)')
        if not code:
            return self.fail('Code fehlt')
        # Google meldet die gewaehrten Scopes ggf. anders als angefragt, oauthlib bricht sonst mit "Scope has changed" ab
        os.environ.setdefault('OAUTHLIB_RELAX_TOKEN_SCOPE', '1')
        try:
            if code.startswith('http'):
                if 'error=' in code and 'code=' not in code:
                    return self.fail('Google hat die Anmeldung abgelehnt: ' + code.split('error=', 1)[1].split('&')[0]
                                     + ' (ist das Konto als Testnutzer eingetragen?)')
                # die Weiterleitung geht auf http://localhost, oauthlib verlangt sonst https
                os.environ.setdefault('OAUTHLIB_INSECURE_TRANSPORT', '1')
                self.flow.fetch_token(authorization_response=code)
            else:
                self.flow.fetch_token(code=code)
        except Exception as e:
            return self.fail(f'Anmeldung fehlgeschlagen: {e}')
        self.save_token(self.flow.credentials)
        self.flow, self.auth_link = None, None
        with self.lock:
            self._service = None
        return self.success('Angemeldet', ui_reload=True)

    # --- Kalender und Farben ------------------------------------------------------------------

    def colors(self) -> dict:
        """ Terminfarben {colorId: {name, background, foreground}}, einmal abgefragt """
        if self._colors is None:
            palette = self.service().colors().get().execute().get('event', {})
            self._colors = {cid: {'name': COLOR_NAMES.get(cid, 'Farbe ' + cid), **c}
                            for cid, c in sorted(palette.items(), key=lambda kv: int(kv[0]))}
        return self._colors

    def calendars(self) -> list:
        items = self.service().calendarList().list().execute().get('items', [])
        return [{'id': c['id'], 'summary': c.get('summaryOverride') or c.get('summary', c['id']),
                 'background': c.get('backgroundColor'), 'foreground': c.get('foregroundColor'),
                 'primary': bool(c.get('primary')), 'time_zone': c.get('timeZone')} for c in items]

    def calendar_color(self, calendar_id) -> dict:
        """ Farbe des Kalenders (gilt fuer Termine ohne colorId) """
        cal = self.service().calendarList().get(calendarId=calendar_id).execute()
        return {'background': cal.get('backgroundColor', '#999999'), 'foreground': cal.get('foregroundColor', '#000000')}

    def time_zone(self, calendar_id) -> str:
        if calendar_id not in self._time_zones:
            self._time_zones[calendar_id] = self.service().calendars().get(calendarId=calendar_id).execute().get('timeZone', 'UTC')
        return self._time_zones[calendar_id]

    def event_color(self, event, calendar_color) -> dict:
        """ wirksame Farbe eines Termins: eigene colorId, sonst die des Kalenders """
        cid = event.get('colorId')
        if cid and cid in self.colors():
            c = self.colors()[cid]
            return {'color_id': cid, 'name': c['name'], 'background': c['background'], 'foreground': c['foreground']}
        return {'color_id': None, 'name': 'Kalenderfarbe', **calendar_color}

    # --- Termine ------------------------------------------------------------------------------

    def events(self, calendar_id=None, days=None, q=None, max_results=50) -> list:
        calendar_id = calendar_id or self.calendar_id
        now = datetime.datetime.now(datetime.timezone.utc)
        end = now + datetime.timedelta(days=int(days or self.days))
        params = dict(calendarId=calendar_id, timeMin=now.isoformat(), timeMax=end.isoformat(), singleEvents=True,
                      orderBy='startTime', maxResults=int(max_results))
        if q:
            params['q'] = q
        items = self.service().events().list(**params).execute().get('items', [])
        cal_color = self.calendar_color(calendar_id)
        res = []
        for e in items:
            start, end_ = e.get('start', {}), e.get('end', {})
            res.append({'id': e['id'], 'summary': e.get('summary', '(ohne Titel)'),
                        'start': start.get('dateTime') or start.get('date'), 'end': end_.get('dateTime') or end_.get('date'),
                        'all_day': 'date' in start, 'location': e.get('location'), 'description': e.get('description'),
                        'link': e.get('htmlLink'), 'color': self.event_color(e, cal_color)})
        return res

    def time_body(self, value, calendar_id) -> dict:
        """ '2026-10-23' -> ganztaegig, '2026-10-23T10:00' -> Uhrzeit in der Zeitzone des Kalenders """
        value = str(value).strip()
        if 'T' not in value:
            datetime.date.fromisoformat(value)
            return {'date': value}
        dt = datetime.datetime.fromisoformat(value)
        if dt.tzinfo is not None:
            return {'dateTime': dt.isoformat()}
        return {'dateTime': dt.isoformat(), 'timeZone': self.time_zone(calendar_id)}

    @staticmethod
    def default_end(start: str) -> str:
        """ ohne Ende: ganztaegig bis zum Folgetag (Ende exklusiv), sonst eine Stunde """
        if 'T' not in start:
            return (datetime.date.fromisoformat(start) + datetime.timedelta(days=1)).isoformat()
        return (datetime.datetime.fromisoformat(start) + datetime.timedelta(hours=1)).isoformat()

    def check_color(self, color_id):
        if color_id and str(color_id) not in self.colors():
            raise ValueError(f'Unbekannte Farbe {color_id}, erlaubt: ' + ', '.join(self.colors().keys()))

    def execute_calendars(self, data):
        return self.success('ok', value=self.calendars())

    def execute_colors(self, data):
        return self.success('ok', value=self.colors())

    def execute_events(self, data):
        return self.success('ok', value=self.events(data.get('calendar_id'), data.get('days'), data.get('q'),
                                                    data.get('max', 50)))

    def execute_create(self, data):
        calendar_id = data.get('calendar_id') or self.calendar_id
        summary, start = str(data.get('summary', '')).strip(), str(data.get('start', '')).strip()
        if not summary or not start:
            return self.fail('summary und start sind erforderlich')
        end = str(data.get('end', '')).strip() or self.default_end(start)
        color_id = str(data.get('color_id', '')).strip()
        try:
            self.check_color(color_id)
            body = {'summary': summary, 'start': self.time_body(start, calendar_id), 'end': self.time_body(end, calendar_id)}
        except ValueError as e:
            return self.fail(str(e))
        if ('date' in body['start']) != ('date' in body['end']):
            return self.fail('start und end muessen beide ganztaegig oder beide mit Uhrzeit sein')
        for key in ('description', 'location'):
            if str(data.get(key, '')).strip():
                body[key] = str(data[key]).strip()
        if color_id:
            body['colorId'] = color_id
        event = self.service().events().insert(calendarId=calendar_id, body=body).execute()
        return self.success('Termin angelegt: ' + summary, value={'id': event['id'], 'link': event.get('htmlLink')})

    def execute_set_color(self, data):
        calendar_id = data.get('calendar_id') or self.calendar_id
        event_id = str(data.get('event_id', '')).strip()
        if not event_id:
            return self.fail('event_id fehlt')
        color_id = str(data.get('color_id', '')).strip()
        try:
            self.check_color(color_id)
        except ValueError as e:
            return self.fail(str(e))
        # None (JSON null) entfernt die eigene Farbe, dann gilt wieder die Kalenderfarbe
        self.service().events().patch(calendarId=calendar_id, eventId=event_id, body={'colorId': color_id or None}).execute()
        name = self.colors()[color_id]['name'] if color_id else 'Kalenderfarbe'
        return self.success('Farbe: ' + name)

    def execute_delete(self, data):
        calendar_id = data.get('calendar_id') or self.calendar_id
        event_id = str(data.get('event_id', '')).strip()
        if not event_id:
            return self.fail('event_id fehlt')
        self.service().events().delete(calendarId=calendar_id, eventId=event_id).execute()
        return self.success('Termin geloescht', ui_reload=True)

    def execute_set_calendar(self, data):
        calendar_id = str(data.get('calendar_id', '')).strip()
        if not calendar_id:
            return self.fail('calendar_id fehlt')
        self.calendar_id = calendar_id
        return self.success('Kalender: ' + calendar_id, ui_reload=True)

    # --- Seite --------------------------------------------------------------------------------

    WEEKDAYS = ['Mo', 'Di', 'Mi', 'Do', 'Fr', 'Sa', 'So']

    @classmethod
    def format_when(cls, event) -> str:
        """ z.B. 'Sa 24.10.2026, ganztägig' bzw. 'Sa 24.10.2026 10:00–11:00' (strftime %a waere vom Locale abhaengig) """
        if event['all_day']:
            day = datetime.date.fromisoformat(event['start'])
            return f"{cls.WEEKDAYS[day.weekday()]} {day.strftime('%d.%m.%Y')}, ganztägig"
        start = datetime.datetime.fromisoformat(event['start'].replace('Z', '+00:00'))
        end = datetime.datetime.fromisoformat(event['end'].replace('Z', '+00:00'))
        return (f"{cls.WEEKDAYS[start.weekday()]} {start.strftime('%d.%m.%Y %H:%M')}–"
                + end.strftime('%H:%M' if end.date() == start.date() else '%d.%m. %H:%M'))

    def html_color_chip(self, color, title=None) -> str:
        return (f'<span title="{html.escape(title or color["name"])}" style="display: inline-block; width: 14px; height: 14px; '
                f'vertical-align: middle; border: 1px solid #0003; background: {html.escape(color["background"])};"></span>')

    @staticmethod
    def color_style(color) -> str:
        return f'background-color: {color["background"]}; color: {color["foreground"]};'

    def html_colored_select(self, current, calendar_color, onchange='', **attrs) -> str:
        """
          Auswahl der Terminfarbe, jede Option in ihrer Farbe, die Auswahl selbst in der gewaehlten
          ('' = Kalenderfarbe). onchange: zusaetzliches JavaScript nach dem Umfaerben
        """
        current = str(current or '')
        options = [('', 'Kalenderfarbe', calendar_color)] + [(cid, c['name'], c) for cid, c in self.colors().items()]
        opts = ''.join(f'<option value="{cid}" style="{html.escape(self.color_style(color))}"'
                       f'{" selected" if cid == current else ""}>{html.escape(name)}</option>'
                       for cid, name, color in options)
        selected = next(color for cid, _, color in options if cid == current) if current in self.colors() else calendar_color
        # beim Wechsel die Farbe der gewaehlten Option uebernehmen
        js = 'const o = this.options[this.selectedIndex]; this.style.backgroundColor = o.style.backgroundColor; ' \
             'this.style.color = o.style.color; ' + onchange
        extra = ''.join(f' {k}="{html.escape(str(v))}"' for k, v in attrs.items())
        return f'<select{extra} style="{html.escape(self.color_style(selected))}" onchange="{html.escape(js)}">{opts}</select>'

    def html_color_select(self, event, calendar_color) -> str:
        """ Terminfarbe eines Termins, wird sofort gesetzt (op=set_color) """
        job = json.dumps({'type': self.type, 'op': 'set_color', 'event_id': event['id'], 'calendar_id': self.calendar_id})
        js = f'exec_job(Object.assign({job}, {{color_id: this.value}}), function(r) {{ location.reload(); }});'
        return self.html_colored_select(event['color']['color_id'], calendar_color, js)

    def part_setup(self, p: b.Page):
        """ noch keine Zugangsdaten: Anleitung mit Links in die Google Cloud Console und Feld zum Einfuegen des JSON """
        with p.section(h="Google Kalender einrichten"):
            p.div("Es sind noch keine Zugangsdaten hinterlegt (" + html.escape(self.credentials_file) + ").")
            p('<ol>'
              '<li>' + w.a("Google Calendar API aktivieren", 'https://console.cloud.google.com/apis/library/calendar-json.googleapis.com')
              + ' (ggf. vorher ein Projekt anlegen)</li>'
              '<li>' + w.a("Google Auth Platform", 'https://console.cloud.google.com/auth/overview')
              + ' einrichten (App-Name, Nutzertyp &bdquo;Extern&ldquo;) und unter '
              + w.a("Zielgruppe &rarr; Testnutzer", 'https://console.cloud.google.com/auth/audience')
              + ' das eigene Google-Konto eintragen</li>'
              '<li>' + w.a("Zugangsdaten anlegen", 'https://console.cloud.google.com/apis/credentials')
              + ': &bdquo;OAuth-Client-ID&ldquo;, Anwendungstyp &bdquo;Desktop-App&ldquo;, danach das JSON herunterladen</li>'
              '<li>Den Inhalt der JSON-Datei hier einfügen und speichern, danach erscheint der Anmelde-Link</li>'
              '</ol>')
            p('<textarea id="gc_credentials" rows="8" style="width: 100%; font-family: monospace;" '
              'placeholder="{&quot;installed&quot;: {&quot;client_id&quot;: ...}}"></textarea>')
            p(self.action_btn_parametric("Speichern", {'type': self.type, 'op': 'save_credentials', 'json': '#gc_credentials'}))
            p.div("Alternativ das JSON eines Dienstkontos, der Kalender muss dann für dessen E-Mail-Adresse freigegeben sein.")

    def part_auth(self, p: b.Page):
        with p.section(h="Anmeldung"):
            try:
                link = self.execute_auth_url({}).get('value')
            except Exception as e:
                p.alert_error('Anmelde-Link nicht erzeugbar: ' + html.escape(str(e)))
                return
            p.div(w.a("Bei Google anmelden", link) if link else "Kein Anmelde-Link")
            p.div("Dem Zugriff zustimmen, danach leitet Google auf http://localhost/?code=... weiter. Die Seite lädt dort "
                  "nicht, das ist normal: die Adresse aus der Adresszeile kopieren und hier einfügen.")
            p.form_input('gc_code', "Adresse bzw. Code", id='gc_code')
            p(self.action_btn_parametric("Anmelden", {'type': self.type, 'op': 'auth_code', 'code': '#gc_code'}))
            p(self.action_btn_parametric("Neuen Link erzeugen", {'type': self.type, 'op': 'auth_url', 'new': 1},
                                         on_success='function(r) { location.reload(); }'))

    def part_index(self, p: b.Page, params={}):
        if not os.path.isfile(self.credentials_file):
            self.part_setup(p)
            return
        try:
            self.credentials_info()
            authorized = self.is_authorized()
        except Exception as e:
            p.alert_error(html.escape(str(e)))
            return
        if not authorized:
            self.part_auth(p)
            return
        try:
            calendars = self.calendars()
            colors = self.colors()
            calendar_color = {'name': 'Kalenderfarbe', **self.calendar_color(self.calendar_id)}
            events = self.events(days=params.get('days'))
        except Exception as e:
            p.alert_error('Google Kalender: ' + html.escape(str(e)))
            return
        with p.section(h="Kalender"):
            p.form_select('gc_calendar', "Kalender", {c['id']: c['summary'] for c in calendars}, self.calendar_id, id='gc_calendar')
            p(self.action_btn_parametric("Auswählen", {'type': self.type, 'op': 'set_calendar', 'calendar_id': '#gc_calendar'}))
        with p.section(h=f"Termine der nächsten {int(params.get('days') or self.days)} Tage"):
            if not events:
                p.div("Keine Termine.")
            for e in events:
                c = e['color']
                title = html.escape(e['summary'])
                if e.get('link'):
                    title = f'<a href="{html.escape(e["link"])}" target="_blank" style="color: inherit;">{title}</a>'
                delete = w.button_js("Löschen", f'if (confirm({json.dumps("Termin " + e["summary"] + " löschen?")})) '
                                                f'{{ exec_job({json.dumps({"type": self.type, "op": "delete", "event_id": e["id"], "calendar_id": self.calendar_id})}); }}')
                p(f'<div style="border-left: 8px solid {html.escape(c["background"])}; padding: 4px 8px; margin: 4px 0;">'
                  f'<div><b>{title}</b> <small>{html.escape(self.format_when(e))}</small></div>'
                  + (f'<div><small>{html.escape(e["location"])}</small></div>' if e.get('location') else '')
                  + f'<div>{self.html_color_chip(c)} {self.html_color_select(e, calendar_color)} {delete}</div></div>')
        with p.section(h="Termin anlegen"):
            p.form_input('gc_summary', "Titel", id='gc_summary')
            p.form_input('gc_start', "Beginn", id='gc_start', type='datetime-local')
            p.form_input('gc_end', "Ende (optional)", id='gc_end', type='datetime-local')
            # wie form_select, aber mit eingefaerbten Optionen
            p('<div class="page_form_input"><span style="width: 150px; display: inline-block;">Farbe</span>'
              + self.html_colored_select('', calendar_color, id='gc_color', name='gc_color') + '</div>')
            p.form_input('gc_location', "Ort", id='gc_location')
            p(self.action_btn_parametric("Anlegen", {'type': self.type, 'op': 'create', 'summary': '#gc_summary',
                                                     'start': '#gc_start', 'end': '#gc_end', 'color_id': '#gc_color',
                                                     'location': '#gc_location'}))
            p.div("Ganztägige Termine per Job: start=2026-10-23 (ohne Uhrzeit).")
        with p.section(h="Farben"):
            p(''.join(f'<span style="display: inline-block; margin: 2px; padding: 2px 8px; '
                      f'background: {html.escape(c["background"])}; color: {html.escape(c["foreground"])};">'
                      f'{cid} {html.escape(c["name"])}</span>' for cid, c in colors.items()))
