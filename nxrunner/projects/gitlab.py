"""
Zugriff auf die GitLab-API eines Projekts.

Die Zugangsdaten stehen in der LOCAL.md des Repos, z.B.::

    gitlab-account: @service_account_...
    gitlab-token: glpat-...
    gitlab-url: https://gitlab.com          (optional, sonst aus dem Remote)
    gitlab-project: user/repo               (optional, sonst aus dem Remote)

Schreibweise gitlab_token oder gitlab-token ist gleichwertig.

Agent: Issues mit den Labels "Agent" und "TODO" werden an einen Runner mit der Signatur
do_issue(name, task_text, path='', wait=False) uebergeben (TODO -> Doing), nach Abschluss wird der Log
als Kommentare ins Issue geschrieben (Doing -> Done).
"""
import datetime
import os
import re
import threading
import time
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote, urlsplit

from nwebclient import base as b
from nwebclient import runner as r
from nwebclient import web as w


class GitlabProjectApi(b.Base):
    """
    GitLab-Projekt zu einem lokalen Repository, Parameter aus LOCAL.md, Projektpfad aus dem Remote
    """

    LOCAL_FILE = 'LOCAL.md'
    PARAM_RE = re.compile(r'^\s*[-*]?\s*(gitlab[-_][\w-]+)\s*[:=]\s*(.*?)\s*$', re.IGNORECASE)
    TIMEOUT = 10

    AGENT_INTERVAL = 3700  # Sekunden zwischen zwei Pruefungen auf neue Agent-Issues
    AGENT_POLL = 30  # Sekunden zwischen zwei Abfragen, ob ein gestartetes Issue fertig ist
    AGENT_SIG = 'do_issue'
    AGENT_SIG_PARAMS = ('name', 'task_text', 'path', 'wait')
    LABEL_AGENT, LABEL_TODO, LABEL_DOING, LABEL_DONE = 'Agent', 'TODO', 'Doing', 'Done'
    NOTE_MAX_CHARS = 8000  # Log wird auf mehrere Kommentare dieser Groesse verteilt

    def __init__(self, path: str, remote_urls: List[str] = None, owner=None, params: dict = None):
        """
        :param owner: BaseJobExecutor (z.B. GitRepo), ueber den der Dispatcher mit den Agent-Runnern gefunden wird
        :param params: gitlab_*-Parameter, ergaenzen bzw. ueberschreiben die aus LOCAL.md
        """
        super().__init__()
        self.path = path
        self.params = {**self.read_params(path), **{k: v for k, v in (params or {}).items() if v}}
        self.remote_urls = remote_urls or []
        if owner is not None:
            owner.addChild(self)
        self.agent_thread = None
        self.agent_runner = None
        self.agent_log = []
        self.agent_running = {}  # iid -> Name des Issues beim Runner

    @classmethod
    def read_params(cls, path: str) -> dict:
        """ gitlab_*-Parameter aus LOCAL.md, Schluessel normalisiert zu gitlab_xxx """
        f = Path(path) / cls.LOCAL_FILE
        if not f.is_file():
            return {}
        res = {}
        for line in f.read_text(encoding='utf-8', errors='replace').splitlines():
            m = cls.PARAM_RE.match(line)
            if m is not None:
                res[m.group(1).lower().replace('-', '_')] = m.group(2)
        return res

    @classmethod
    def is_configured(cls, path: str) -> bool:
        return len(cls.read_params(path)) > 0

    def token(self) -> Optional[str]:
        return self.params.get('gitlab_token')

    def remote(self) -> Optional[tuple]:
        """ (base_url, project_path) aus dem ersten passenden Remote """
        from nxrunner.projects.dev import GitRepo
        for url in self.remote_urls:
            web = GitRepo.remote_web_url(url)
            if web is not None:
                parts = urlsplit(web)
                if 'gitlab' in (parts.hostname or '') or 'gitlab_url' in self.params:
                    return f'{parts.scheme}://{parts.netloc}', parts.path.strip('/')
        return None

    def base_url(self) -> str:
        if 'gitlab_url' in self.params:
            return self.params['gitlab_url'].rstrip('/')
        rem = self.remote()
        return rem[0] if rem is not None else 'https://gitlab.com'

    def project_path(self) -> Optional[str]:
        if 'gitlab_project' in self.params:
            return self.params['gitlab_project'].strip('/')
        rem = self.remote()
        return rem[1] if rem is not None else None

    def web_url(self) -> Optional[str]:
        pp = self.project_path()
        return f'{self.base_url()}/{pp}' if pp is not None else None

    def request(self, method: str, endpoint: str = '', params: dict = None, data: dict = None):
        """ HTTP-Aufruf auf /api/v4/projects/:id{endpoint} """
        import requests
        pp = self.project_path()
        if pp is None:
            raise ValueError("GitLab-Projekt unbekannt (gitlab-project in LOCAL.md oder GitLab-Remote noetig)")
        url = f"{self.base_url()}/api/v4/projects/{quote(pp, safe='')}{endpoint}"
        headers = {'PRIVATE-TOKEN': self.token()} if self.token() else {}
        resp = requests.request(method, url, headers=headers, params=params, json=data, timeout=self.TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def get(self, endpoint: str = '', **params):
        """ GET auf /api/v4/projects/:id{endpoint} """
        return self.request('GET', endpoint, params=params)

    def put(self, endpoint: str, **data):
        return self.request('PUT', endpoint, data=data)

    def post(self, endpoint: str, **data):
        return self.request('POST', endpoint, data=data)

    def project(self) -> dict:
        return self.get()

    def pipelines(self, limit: int = 10) -> List[dict]:
        return self.get('/pipelines', per_page=limit)

    def issues(self, limit: int = 20) -> List[dict]:
        return self.get('/issues', state='opened', per_page=limit)

    def merge_requests(self, limit: int = 20) -> List[dict]:
        return self.get('/merge_requests', state='opened', per_page=limit)

    def issues_with_labels(self, labels: List[str], limit: int = 100) -> List[dict]:
        """ offene Issues, die alle labels tragen """
        return self.get('/issues', state='opened', labels=','.join(labels), per_page=limit)

    def change_labels(self, iid: int, add: str = '', remove: str = '') -> dict:
        return self.put(f'/issues/{iid}', add_labels=add, remove_labels=remove)

    def add_note(self, iid: int, body: str) -> dict:
        return self.post(f'/issues/{iid}/notes', body=body)

    # --- Agent --------------------------------------------------------------

    def info(self, msg: str):
        """ Eintrag im Agent-Log (Anzeige im Reiter Agent) """
        self.agent_log.append(datetime.datetime.now().strftime('%H:%M:%S ') + str(msg))
        del self.agent_log[:-100]

    def dispatcher(self):
        from nwebclient import runner as r
        return self.getParentClass(r.LazyDispatcher)

    @classmethod
    def has_agent_sig(cls, runner) -> bool:
        """ True, wenn runner do_issue(name, task_text, path, wait) bereitstellt """
        sigs = getattr(runner, 'signatures', None)
        if sigs is None or not sigs.contains_name(cls.AGENT_SIG):
            return False
        func = sigs[cls.AGENT_SIG]
        return all(func.contains_name(n) for n in cls.AGENT_SIG_PARAMS)

    def agent_runners(self) -> dict:
        """ {name: runner} aller erzeugten Runner im Dispatcher mit der Signatur do_issue """
        disp = self.dispatcher()
        if disp is None:
            return {}
        entries = dict(getattr(disp, 'entries', None) or {})
        return {name: e.instance for name, e in entries.items() if e.loaded and self.has_agent_sig(e.instance)}

    def agent_active(self) -> bool:
        return self.agent_thread is not None

    def agent_start(self, runner_name: str, check: bool = True, first_delay: float = 0) -> str:
        """
        prueft periodisch (AGENT_INTERVAL) auf neue Agent-Issues und gibt sie an runner_name

        :param runner_name: '' = erster gefundener Runner mit do_issue (bei jeder Pruefung neu gesucht)
        :param check: Runner muss jetzt schon im Dispatcher erzeugt sein
        :param first_delay: Sekunden bis zur ersten Pruefung (z.B. bis alle Runner geladen sind)
        """
        if check and runner_name and runner_name not in self.agent_runners():
            raise ValueError(f"Runner {runner_name} mit {self.AGENT_SIG} nicht gefunden")
        self.agent_stop()
        self.agent_runner = runner_name
        self.agent_thread = self.periodic(self.AGENT_INTERVAL, self.do_issues, daemon=True)
        self.info(f"Agent aktiviert mit {runner_name or '(erster Runner)'}, alle {self.AGENT_INTERVAL}s")
        if first_delay > 0:
            self.delayed(first_delay, self.do_issues_safe)
        else:
            threading.Thread(target=self.do_issues_safe, daemon=True).start()
        return runner_name or '(erster Runner)'

    def do_issues_safe(self):
        try:
            self.do_issues()
        except Exception as e:
            self.info(f"Pruefung fehlgeschlagen: {e}")

    def agent_stop(self):
        if self.agent_thread is not None:
            self.agent_thread.stop()
            self.agent_thread = None
            self.info("Agent deaktiviert")

    def do_issues(self, runner_name: str = None) -> List[int]:
        """
        Ruft die offenen Issues mit Agent und TODO ab und startet sie beim Runner.

        :return: iids der gestarteten Issues
        """
        runner_name = runner_name or self.agent_runner
        runners = self.agent_runners()
        if not runner_name and len(runners) > 0:
            runner_name = next(iter(runners))
        runner = runners.get(runner_name)
        if runner is None:
            self.info(f"Runner {runner_name} nicht verfuegbar")
            return []
        started = []
        for issue in self.issues_with_labels([self.LABEL_AGENT, self.LABEL_TODO]):
            if issue['iid'] not in self.agent_running and self.start_issue(runner, issue):
                started.append(issue['iid'])
        self.info(f"Pruefung: {len(started)} Issue(s) gestartet")
        return started

    @staticmethod
    def issue_name(issue: dict) -> str:
        """ Name fuer den Runner (Branch feat_<name>), z.B. 12_fix_login """
        slug = re.sub(r'[^a-z0-9]+', '_', issue.get('title', '').lower()).strip('_')[:40]
        return f"{issue['iid']}_{slug}".rstrip('_')

    @staticmethod
    def issue_task(issue: dict) -> str:
        return f"{issue.get('title', '')}\n\n{issue.get('description') or ''}".strip()

    def start_issue(self, runner, issue: dict) -> bool:
        """ TODO -> Doing, do_issue(wait=False), danach Ueberwachung in eigenem Thread """
        iid, name = issue['iid'], self.issue_name(issue)
        self.change_labels(iid, add=self.LABEL_DOING, remove=self.LABEL_TODO)
        res = runner.execute({'op': self.AGENT_SIG, 'name': name, 'task_text': self.issue_task(issue),
                              'path': str(self.path), 'wait': False})
        if not isinstance(res, dict) or not res.get('success', False):
            msg = res.get('message', res) if isinstance(res, dict) else res
            self.info(f"#{iid} nicht gestartet: {msg}")
            self.add_note(iid, f"Agent konnte nicht starten: {msg}")
            self.change_labels(iid, add=self.LABEL_TODO, remove=self.LABEL_DOING)
            return False
        self.agent_running[iid] = name
        self.info(f"#{iid} gestartet als {name}")
        threading.Thread(target=self.watch_issue, args=(runner, iid, name, res.get('issue')), daemon=True).start()
        return True

    def runner_issue_state(self, runner, name: str, issue: Optional[dict]) -> Optional[dict]:
        """ aktueller Stand beim Runner: per list_issues (falls vorhanden), sonst das gelieferte issue-dict """
        if runner.has_signature('list_issues') or getattr(runner, 'execute_list_issues', None) is not None:
            try:
                res = runner.execute({'op': 'list_issues'})
                for itm in res.get('issues', []):
                    if itm.get('name') == name:
                        return itm
            except Exception as e:
                self.info(f"list_issues: {e}")
        return issue

    def watch_issue(self, runner, iid: int, name: str, issue: Optional[dict]):
        """ wartet, bis das Issue beim Runner fertig ist, und schreibt dann das Ergebnis ins GitLab-Issue """
        state = issue
        while True:
            state = self.runner_issue_state(runner, name, state)
            if state is None or state.get('finished'):
                break
            time.sleep(self.AGENT_POLL)
        try:
            self.finish_issue(iid, state)
        except Exception as e:
            self.info(f"#{iid} Abschluss fehlgeschlagen: {e}")
        finally:
            self.agent_running.pop(iid, None)

    def finish_issue(self, iid: int, state: Optional[dict]):
        """ Log als Kommentare, Doing -> Done """
        for note in self.log_notes(state):
            self.add_note(iid, note)
        self.change_labels(iid, add=self.LABEL_DONE, remove=self.LABEL_DOING)
        self.info(f"#{iid} fertig")

    @classmethod
    def log_notes(cls, state: Optional[dict]) -> List[str]:
        """ Kommentartexte: Zusammenfassung und der Log, verteilt auf Bloecke <= NOTE_MAX_CHARS """
        if state is None:
            return ["Agent: kein Stand vom Runner verfuegbar"]
        head = (f"**Agent:** {state.get('status', '')} (success: {state.get('success')})  \n"
                f"Branch: `{state.get('branch', '')}` Commit: `{state.get('commit', '') or '-'}`")
        notes = [head]
        if state.get('response'):
            notes += [f"**Antwort**\n\n{chunk}" for chunk in cls.chunks(str(state['response']).splitlines())]
        log = [str(line) for line in state.get('log', [])]
        notes += [f"**Log**\n\n```\n{chunk}\n```" for chunk in cls.chunks(log)]
        return notes

    @classmethod
    def chunks(cls, lines: List[str]) -> List[str]:
        """ Zeilen zu Bloecken mit hoechstens NOTE_MAX_CHARS Zeichen (lange Zeilen werden geteilt) """
        n = cls.NOTE_MAX_CHARS
        pieces = [line[i:i + n] for line in lines for i in range(0, max(len(line), 1), n)]
        res, cur = [], None
        for piece in pieces:
            if cur is not None and len(cur) + len(piece) + 1 > n:
                res.append(cur)
                cur = None
            cur = piece if cur is None else f"{cur}\n{piece}"
        return res + ([cur] if cur else [])

    # --- Agent per Job (Aufruf ueber die Ops des Owners) ----------------------

    def agent_job(self, action: str, runner: str = '') -> dict:
        """ action: start, stop, do_issues """
        try:
            if action == 'start':
                return {'success': True, 'message': f"Agent aktiv mit {self.agent_start(runner)}"}
            if action == 'stop':
                self.agent_stop()
                return {'success': True, 'message': "Agent deaktiviert"}
            if action == 'do_issues':
                started = self.do_issues(runner or None)
                return {'success': True, 'message': f"{len(started)} Issue(s) gestartet", 'started': started}
            return {'success': False, 'message': f"unbekannte Aktion {action}"}
        except Exception as e:
            self.info(f"{action}: {e}")
            return {'success': False, 'message': str(e)}

    # --- HTML ---------------------------------------------------------------

    def part_summary(self, p: b.Page):
        """ kurze Uebersicht fuer die Index-Seite (ohne API-Aufruf) """
        p.prop("Projekt", w.s(self.project_path() or '-'))
        p.prop("Account", w.s(self.params.get('gitlab_account', '-')))
        p.prop("Token", "gesetzt" if self.token() else "fehlt")
        if self.web_url() is not None:
            p.div(w.a("GitLab öffnen", self.web_url(), hint="Projekt auf GitLab im Browser öffnen"))

    def part_gitlab(self, p: b.Page, params={}):
        """ Detailseite als Reiter: Projekt, Pipelines, Merge Requests, Issues """
        p.h2("GitLab")
        tabs = [("Projekt", self.part_project, "Projektdaten und Zugangsdaten aus LOCAL.md"),
                ("Pipelines", self.part_pipelines, "Letzte CI-Pipelines"),
                ("Merge Requests", self.part_merge_requests, "Offene Merge Requests"),
                ("Issues", self.part_issues, "Offene Issues"),
                ("Agent", self.part_agent, "Issues mit Label Agent+TODO automatisch von einem Runner bearbeiten")]
        with p.tabs() as t:
            for title, part, hint in tabs:
                with t(title, hint=hint):
                    self.part_safe(p, part)

    @staticmethod
    def part_safe(p: b.Page, part):
        """ Fehler der API im Reiter anzeigen, statt die ganze Seite abzubrechen """
        try:
            part(p)
        except Exception as e:
            p.alert_error(w.s(f"GitLab-API: {e}"))

    def part_project(self, p: b.Page):
        self.part_summary(p)
        prj = self.project()
        p.prop("Name", w.s(prj.get('name_with_namespace', '')))
        p.prop("Default Branch", w.s(prj.get('default_branch', '')))
        p.prop("Letzte Aktivität", w.s(prj.get('last_activity_at', '')))
        p.prop("Offene Issues", prj.get('open_issues_count', '-'))

    def part_pipelines(self, p: b.Page):
        rows = [['ID', 'Status', 'Ref', 'Erstellt']]
        for pl in self.pipelines():
            rows.append([w.a(f"#{pl['id']}", pl.get('web_url', '#'), hint="Pipeline auf GitLab öffnen"),
                         w.s(pl.get('status', '')), w.s(pl.get('ref', '')), w.s(pl.get('created_at', ''))])
        p(w.table(rows))

    def part_merge_requests(self, p: b.Page):
        self.part_item_list(p, self.merge_requests(), '!')

    def part_issues(self, p: b.Page):
        self.part_item_list(p, self.issues(), '#')

    def agent_btn(self, title: str, hint: str, **data) -> str:
        """ Button, der die Op gitlab_agent des Owners ausfuehrt """
        owner = self.owner()
        return owner.action_btn(dict(title=title, type=owner.type, op='gitlab_agent', **data), hint=hint)

    def part_agent(self, p: b.Page):
        p.prop("Status", f"aktiv mit {w.s(self.agent_runner)}" if self.agent_active() else "inaktiv")
        p.prop("Intervall", f"{self.AGENT_INTERVAL} s")
        p.prop("Labels", f"{self.LABEL_AGENT} + {self.LABEL_TODO} → {self.LABEL_DOING} → {self.LABEL_DONE}")
        p.prop("In Arbeit", w.s(', '.join(f"#{iid} {n}" for iid, n in self.agent_running.items()) or '-'))
        if self.agent_active():
            p.div(self.agent_btn("Deaktivieren", "Periodische Prüfung auf Agent-Issues beenden", action='stop'))
        self.part_agent_runners(p)
        if self.agent_log:
            p.h3("Log")
            p.pre(w.s('\n'.join(reversed(self.agent_log))))

    def part_agent_runners(self, p: b.Page):
        p.h3(f"Runner mit {self.AGENT_SIG}")
        runners = self.agent_runners()
        if len(runners) == 0:
            p.div(f"Kein erzeugter Runner mit {self.AGENT_SIG}(name, task_text, path, wait) gefunden")
            return
        rows = [['Runner', 'Klasse', '']]
        for name, runner in runners.items():
            btns = self.agent_btn("Aktivieren", f"Alle {self.AGENT_INTERVAL}s neue Agent-Issues an {name} geben",
                                  action='start', runner=name)
            btns += self.agent_btn("Do Issues", f"Einmalig Agent+TODO-Issues abrufen und an {name} geben",
                                   action='do_issues', runner=name)
            rows.append([w.s(name), w.s(type(runner).__name__), btns])
        p(w.table(rows))

    @staticmethod
    def part_item_list(p: b.Page, items: List[dict], prefix: str):
        if len(items) == 0:
            p.div("keine offenen")
        p.ul([w.a(f"{prefix}{itm['iid']} " + w.s(itm.get('title', '')), itm.get('web_url', '#'),
                  hint="Auf GitLab öffnen") for itm in items])


class GitlabAgentRunner(r.BaseJobExecutor):
    """
    Agent-Loop fuer ein GitLab-Projekt: Issues mit den Labels Agent und TODO werden periodisch an einen Runner
    mit do_issue(name, task_text, path, wait) gegeben, der Log landet als Kommentare im Issue.

    Ohne Zugangsdaten zeigt part_index eine Anleitung zur Einrichtung.
    """

    TAGS = ['agent', 'gitlab']
    START_DELAY = 30  # Sekunden bis zur ersten Pruefung, damit die Runner des Dispatchers geladen sind

    def __init__(self, type='gitlab_agent', token: str = None, project: str = None, url: str = None,
                 account: str = None, path: str = '.', runner: str = '', interval: int = None, start: bool = True):
        """
        :param token: Access Token mit Scope api
        :param project: Projektpfad, z.B. gruppe/repo (sonst aus LOCAL.md in path)
        :param url: GitLab-Instanz, Standard https://gitlab.com
        :param path: lokales Repository, in dem der Agent-Runner arbeitet (do_issue path=...)
        :param runner: Name des Runners mit do_issue, '' = erster gefundener
        :param interval: Sekunden zwischen zwei Pruefungen, Standard GitlabProjectApi.AGENT_INTERVAL
        :param start: Agent-Loop beim Erzeugen starten (wenn Zugangsdaten vorhanden)
        """
        super().__init__(type)
        self.path = os.path.abspath(path)
        params = {'gitlab_token': token, 'gitlab_project': project, 'gitlab_url': url, 'gitlab_account': account}
        self.api = GitlabProjectApi(self.path, [], owner=self, params=params)
        if interval:
            self.api.AGENT_INTERVAL = int(interval)
        if start and self.configured():
            self.api.agent_start(runner, check=False, first_delay=self.START_DELAY)

    def configured(self) -> bool:
        """ Token und Projekt bekannt """
        return bool(self.api.token()) and self.api.project_path() is not None

    def missing(self) -> List[str]:
        res = [] if self.api.token() else ['token']
        return res + ([] if self.api.project_path() is not None else ['project'])

    @r.op("Agent: action=start (periodisch, runner), stop oder do_issues (einmalig, runner)",
          result={'success': 'bool', 'message': 'str'})
    def gitlab_agent(self, action: str, runner: str = ''):
        if not self.configured():
            return self.fail("Zugangsdaten fehlen: " + ', '.join(self.missing()))
        return self.api.agent_job(action, runner)

    @r.op("Status des Agents", result={'success': 'bool', 'active': 'bool', 'runner': 'str', 'running': 'dict'})
    def agent_status(self):
        return self.success('ok', configured=self.configured(), active=self.api.agent_active(),
                            runner=self.api.agent_runner or '', running=dict(self.api.agent_running),
                            runners=list(self.api.agent_runners().keys()), log=self.api.agent_log[-20:])

    def part_index(self, p: b.Page, params={}):
        if not self.configured():
            self.part_setup_help(p)
            return
        p.h2("GitLab Agent")
        self.api.part_summary(p)
        p.div(w.a("Pipelines, MRs, Issues", self.link(self.part_gitlab), hint="GitLab-Details per API laden"))
        self.api.part_agent(p)

    def part_gitlab(self, p: b.Page, params={}):
        self.api.part_gitlab(p, params)

    def part_setup_help(self, p: b.Page):
        p.alert_error(w.s("Es fehlen: " + ', '.join(self.missing())))
        p.h2("GitLab Agent einrichten")
        with p.section(h="1. Access Token anlegen"):
            p.ul(["GitLab: Projekt → Settings → Access Tokens (oder Benutzer → Preferences → Access Tokens)",
                  "Scope <code>api</code>, Rolle mindestens Developer (Labels setzen, Kommentare schreiben)"])
        with p.section(h="2. Runner konfigurieren"):
            p.pre(w.s(f'{self.type}: nxrunner.projects.gitlab:GitlabAgentRunner(token="glpat-...", '
                      f'project="gruppe/repo", path="/pfad/zum/repo")'))
            p.ul(["<code>url</code>: eigene GitLab-Instanz (Standard https://gitlab.com)",
                  "<code>runner</code>: Name des Agent-Runners, leer = erster gefundener",
                  "<code>interval</code>: Sekunden zwischen zwei Prüfungen (Standard "
                  f"{GitlabProjectApi.AGENT_INTERVAL})",
                  f"Alternativ <code>gitlab-token</code> / <code>gitlab-project</code> in {GitlabProjectApi.LOCAL_FILE} "
                  "im Repo <code>path</code>"])
        self.part_setup_runner_help(p)

    def part_setup_runner_help(self, p: b.Page):
        api = self.api
        with p.section(h=f"3. Runner mit {api.AGENT_SIG}"):
            p.div(w.s(f"Benötigt wird ein Runner mit {api.AGENT_SIG}(name: str, task_text: str, path: str = '', "
                      "wait: bool = False), z.B. nxml.agents:OpenCodeAgent."))
            found = list(api.agent_runners().keys())
            p.prop("Gefunden", w.s(', '.join(found) or 'keiner'))
            p.h3("4. Labels im Projekt")
            p.div(w.s(f"Issues mit {api.LABEL_AGENT} + {api.LABEL_TODO} werden bearbeitet: "
                      f"{api.LABEL_TODO} → {api.LABEL_DOING} → {api.LABEL_DONE}. "
                      "Die Labels im Projekt anlegen (Manage → Labels)."))
