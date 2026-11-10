"""
Zugriff auf die GitLab-API eines Projekts.

Die Zugangsdaten stehen in der LOCAL.md des Repos, z.B.::

    gitlab-account: @service_account_...
    gitlab-token: glpat-...
    gitlab-url: https://gitlab.com          (optional, sonst aus dem Remote)
    gitlab-project: user/repo               (optional, sonst aus dem Remote)

Schreibweise gitlab_token oder gitlab-token ist gleichwertig.
"""
import re
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote, urlsplit

from nwebclient import base as b
from nwebclient import web as w


class GitlabProjectApi:
    """
    GitLab-Projekt zu einem lokalen Repository, Parameter aus LOCAL.md, Projektpfad aus dem Remote
    """

    LOCAL_FILE = 'LOCAL.md'
    PARAM_RE = re.compile(r'^\s*[-*]?\s*(gitlab[-_][\w-]+)\s*[:=]\s*(.*?)\s*$', re.IGNORECASE)
    TIMEOUT = 10

    def __init__(self, path: str, remote_urls: List[str] = None):
        self.path = path
        self.params = self.read_params(path)
        self.remote_urls = remote_urls or []

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

    def get(self, endpoint: str = '', **params):
        """ GET auf /api/v4/projects/:id{endpoint} """
        import requests
        pp = self.project_path()
        if pp is None:
            raise ValueError("GitLab-Projekt unbekannt (gitlab-project in LOCAL.md oder GitLab-Remote noetig)")
        url = f"{self.base_url()}/api/v4/projects/{quote(pp, safe='')}{endpoint}"
        headers = {'PRIVATE-TOKEN': self.token()} if self.token() else {}
        resp = requests.get(url, headers=headers, params=params, timeout=self.TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def project(self) -> dict:
        return self.get()

    def pipelines(self, limit: int = 10) -> List[dict]:
        return self.get('/pipelines', per_page=limit)

    def issues(self, limit: int = 20) -> List[dict]:
        return self.get('/issues', state='opened', per_page=limit)

    def merge_requests(self, limit: int = 20) -> List[dict]:
        return self.get('/merge_requests', state='opened', per_page=limit)

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
                ("Issues", self.part_issues, "Offene Issues")]
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

    @staticmethod
    def part_item_list(p: b.Page, items: List[dict], prefix: str):
        if len(items) == 0:
            p.div("keine offenen")
        p.ul([w.a(f"{prefix}{itm['iid']} " + w.s(itm.get('title', '')), itm.get('web_url', '#'),
                  hint="Auf GitLab öffnen") for itm in items])
