"""
  python -m nxbot.dev ci jobname

  before Python 3.10 install tomli
"""
import time
import subprocess
import html
import concurrent.futures
from urllib.parse import parse_qs, urlsplit
import json
import re
import glob
import os.path
from pathlib import Path
from fnmatch import fnmatch
from typing import List, Optional

from nwebclient import runner as r, base
from nwebclient import util as u
from nwebclient import base as b
from nwebclient import web as w
from nwebclient import dev


SOURCE_EXTS = ['java', 'py', 'html', 'css', 'js', 'ts', 'php', 'rt', 'kt', 'h', 'c', 'cpp', 'rs']

DOC_EXTS = ['md', 'txt']


def is_source_file(path: str):
    for ext in SOURCE_EXTS:
        if path.lower().endswith('.'+ext):
            return True
    return False


def is_doc_file(path: str):
    for ext in DOC_EXTS:
        if path.lower().endswith('.'+ext):
            return True
    return False


def is_file_cat(path: str, source=False, doc=False):
    if source and is_source_file(path):
        return True
    if doc and is_doc_file(path):
        return True
    return False


def _exclude_filter_param(params: dict) -> bool:
    """
    Zustand der Checkbox "exclude": beim ersten Aufruf (ohne q) an, nach dem Absenden wie angehakt
    """
    if 'q' not in params:
        return True
    return params.get('exclude', '') not in ('', '0')


def _exclude_filter_checkbox(checked: bool) -> str:
    return (f'<label title="Verzeichnisse wie build/ oder *.egg-info ueberspringen">'
            f'<input type="checkbox" name="exclude" value="1"{" checked" if checked else ""}> exclude_filter</label> ')


class Projects(r.BaseJobExecutor):
    """
    Verwaltet die in nweb.json:repos definierten Projekte, laedt sie als passende Projekt-Runner und bietet Grid, Suche, Abhaengigkeitsgraph und paralleles git pull.
    """

    def __init__(self, args: u.Args = {}):
        super().__init__('projects')
        self.define_vars('dirty_project_count')
        if args is None:
            args = u.Args()
        self.args = args
        self._dirty_project_count = None  # Cache fuer get_dirty_project_count()
        self._dirty_project_count_time = 0.0

    def execute(self, data):
        if 'load' in data:
            return self.load(data['load'])
        return super().execute(data)

    def load(self, name):
        return self.onParentClass(r.LazyDispatcher, lambda d: self.load_project(d, name), {'success': False})

    def create_project(self, name: str) -> 'ProjectDir':
        path = self.args.get('repos', {}).get(name, name)
        if os.path.isfile(path + '/platformio.ini'):
            from nxbot.esp import PioProject
            runner = PioProject(path)
        elif (os.path.isfile(path + '/setup.py') or os.path.isfile(path + '/pyproject.toml')) and os.path.isdir(path + '/.git'):
            runner = PythonRepo(path)
        elif compose_file(path) is not None:
            runner = DockerCompose(path)
        elif os.path.isdir(path + '/.git'):
            runner = GitRepo(path)
        else:
            runner = ProjectDir(path)
        runner.type = name  # Runner ist unter dem Namen aus nweb.json:repos erreichbar
        return runner

    def load_project(self, d: r.LazyDispatcher, name):
        if not d.support_type(name):
            d.add_runner(self.create_project(name))
        return self.success('loaded', ui_reload=True)

    def part_project(self, p: base.Page, params={}):
        name = params['name']
        p(self.onParentClass(r.LazyDispatcher, lambda d: self.on_parent_project(d, name), ''))
        p.hr()
        p.div(w.a("Projects", self.link('index')))

    def on_parent_project(self, d: r.LazyDispatcher, name):
        if d.support_type(name):
            return w.a("Go to", f'?type={name}')
        else:
            return self.action_btn({'title': "Load", 'type': self.type, 'load': name})

    def part_index(self, p: base.Page, params={}):
        repos = self.args.get('repos', {})
        p.nav(
            w.a("Grid", self.link(self.part_grid)),
            w.a("Git Actions", self.link(self.part_gitactions)),
            w.a("Search", self.link(self.part_search)),
            w.a("Graph", self.link(self.part_graph))
        )
        p.right(self.action_btn(dict(title="Pull All", type=self.type, op='pull_all')))
        p("Zeigt die in der nweb.json:repos definierten Projekte an.")

        with p.section(h="Projects"):
            for name in repos.keys():
                path = repos[name]
                p.div(w.a(name, self.link('project', f'name={name}')), title=path)
        self.part_system(p, params)
        p.pre('', id='result')

    def execute_pull_all(self, data={}):
        """
        Fuehrt in jedem Git-Projekt aus nweb.json:repos parallel ein git pull aus
        """
        projects = [project for project in self.expand_projects() if project.exists('.git')]

        def pull(project: 'ProjectDir'):
            try:
                return project.type, project.execute_pull({})
            except Exception as e:
                return project.type, self.fail(str(e))

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = dict(pool.map(pull, projects))
        failed = [name for name, res in results.items() if not res.get('success', False)]
        return self.success(f"Pulled {len(results) - len(failed)} / {len(results)} Projects",
                            failed=failed, results=results)

    def expand_projects(self) -> List['ProjectDir']:
        """
        Fuegt alle Projekte aus nweb.json:repos als Runner hinzu

        :return: Alle Projekt-Runner
        """
        def expand(d: r.LazyDispatcher):
            res = []
            for name in self.args.get('repos', {}).keys():
                try:
                    self.load_project(d, name)
                    res.append(d.get_runner(name))
                except Exception as e:
                    self.error(f"Unable to load project {name}: {e}")
            return res
        return self.onParentClass(r.LazyDispatcher, expand, [])

    def get(self, name: str) -> 'ProjectDir':
        """
        Projekt-Runner zu einem Namen aus nweb.json:repos, wird bei Bedarf geladen

        Ohne LazyDispatcher als Parent wird ein neuer, nicht registrierter Runner erzeugt.

        :param name: Projektname aus nweb.json:repos
        :raises KeyError: Wenn das Projekt nicht in nweb.json:repos definiert ist
        """
        if name not in self.args.get('repos', {}):
            raise KeyError(f"Unknown project: {name}")

        def load(d: r.LazyDispatcher):
            self.load_project(d, name)
            return d.get_runner(name)
        project = self.onParentClass(r.LazyDispatcher, load, None)
        return project if project is not None else self.create_project(name)

    def get_dirty_project_count(self, max_age: float = 300) -> int:
        """
        Anzahl der Git-Projekte mit uncommitteten Aenderungen oder nicht gepushten Commits (gecacht)

        Wird bei jedem Lesen der Web-Variable dirty_project_count aufgerufen, daher wird der Wert
        zwischengespeichert und erst neu berechnet (count_dirty_projects()), wenn er aelter als max_age Sekunden ist.

        :param max_age: Maximales Alter des gecachten Werts in Sekunden
        """
        now = time.time()
        if self._dirty_project_count is None or now - self._dirty_project_count_time > max_age:
            self._dirty_project_count = self.count_dirty_projects()
            self._dirty_project_count_time = now
        return self._dirty_project_count

    def count_dirty_projects(self) -> int:
        """
        Zaehlt die Git-Projekte mit uncommitteten Aenderungen (auch neue Dateien) oder nicht gepushten Commits

        Nicht gepusht = Commits vor dem Tracking-Branch (ohne fetch, wie GitRepo.status()).
        Projekte ohne Git oder deren Status nicht lesbar ist werden nicht gezaehlt.
        Laedt alle Projekte und fuehrt git status aus (teuer), siehe get_dirty_project_count().
        """
        count = 0
        for project in self.expand_projects():
            if not isinstance(project, GitRepo):
                continue
            try:
                s = project.status()
            except Exception as e:
                self.error(f"count_dirty_projects {project.type}: {e}")
                continue
            if s['dirty'] or s['ahead'] > 0:
                count += 1
        return count

    def product_type(self, project: 'ProjectDir') -> str:
        try:
            return project.product_type()
        except Exception as e:
            self.error(f"product_type of {project.type}: {e}")
            return ''

    def project_status(self, project: 'ProjectDir') -> str:
        if not isinstance(project, GitRepo):
            return ''
        try:
            s = project.status()
            attrs = {'href': project.link(project.part_status), 'title': html.escape(project.status_hint(s))}
            if project.is_up_to_date(s):
                attrs['style'] = 'color: green;'
            return w.tag('a', project.status_short(s), **attrs)
        except Exception as e:
            return 'Error: ' + str(e)

    def execute_project_status(self, data):
        """
        Kurzstatus eines Projekts als HTML, wird vom Grid per Ajax nachgeladen

        :param name: Projektname aus nweb.json:repos
        """
        project = self.onParentClass(r.LazyDispatcher, lambda d: d.get_runner(data.get('name', '')), None)
        if project is None:
            return self.fail("Unknown project")
        return self.success('', html=self.project_status(project))

    def part_grid(self, p: base.Page, params={}):
        """
        Liste aller Projekte, filterbar nach Name/Pfad (q), Tag (tag) und Projektart (kind)
        """
        projects = self.expand_projects()
        products = {project.type: self.product_type(project) for project in projects}

        def kind(project):
            return project.__class__.__name__

        def is_git(project):
            return isinstance(project, GitRepo)

        def tags(project):
            return getattr(project, 'TAGS', [])  # Runner ohne TAGS haben keine Tags
        p.grid(projects, [
            w.Grid.col('type', "Name", value=lambda pr: w.a(pr.type, f'?type={pr.type}'), html=True),
            w.Grid.col('kind', "Type", value=kind, sort=kind),
            w.Grid.col('product', "Product", html=True, sort=lambda pr: products[pr.type],
                       value=lambda pr: w.a(products[pr.type], self.link('grid', product=products[pr.type])) if products[pr.type] != '' else ''),
            w.Grid.col('TAGS', "Tags", html=True, sortable=False,
                       value=lambda pr: ', '.join(map(lambda t: w.a(t, self.link('grid', tag=t)), tags(pr)))),
            w.Grid.col('status', '<span title="↑ ahead, ↓ behind, * geänderte Dateien, ✓ sauber">Status</span>', sortable=False,
                       value=lambda pr: '...' if is_git(pr) else '',
                       td={'_class': lambda pr: 'project_status' if is_git(pr) else '', 'data_name': lambda pr: pr.type}),
            w.Grid.col('path', "Path"),
        ], params, search=lambda pr: pr.type + ' ' + pr.path, count="{shown} / {total} Projects", filters={
            'tag': ("All Tags", tags),
            'kind': ("All Types", kind),
            'product': ("All Products", lambda pr: products[pr.type]),
        })
        p.js_ready(self.js_project_status())
        p.div(w.a("Projects", self.link('index')))

    def js_project_status(self) -> str:
        """
        JS: laedt den Status in alle Zellen td.project_status (data-name = Projekt), projects_load_status(td) laedt neu
        """
        return """
            window.projects_load_status = function(td) {
                const data = {type: '""" + self.type + """', op: 'project_status', name: td.dataset.name};
                fetch(base_url(), post(data)).then((response) => response.json()).then((res) => {
                    td.innerHTML = res.success ? res.html : ('Error: ' + res.message);
                }).catch((e) => { td.textContent = 'Error: ' + e; });
            };
            document.querySelectorAll("td.project_status").forEach(projects_load_status);
        """

    # op des Projekt-Runners, Titel, Tooltip
    GIT_ACTIONS = [
        ('up', "Up", "Commit aller Aenderungen an versionierten Dateien (git commit -am sc) und git push. "
                     "Neue Dateien werden nicht hinzugefuegt."),
        ('pull', "Pull", "git pull"),
    ]

    @staticmethod
    def project_remotes(project: 'GitRepo') -> str:
        try:
            return '<br>'.join(f"{w.s(rm['name'])}: {w.s(rm['url'])}" for rm in project.remotes())
        except Exception as e:
            return 'Error: ' + w.s(str(e))

    @staticmethod
    def project_web(project: 'GitRepo') -> str:
        """
        Links zu den Webseiten der Remotes (GitHub, GitLab, Codeberg, ...)
        """
        try:
            return ' '.join(f'<a href="{w.s(link["url"])}" target="_blank" rel="noopener" '
                            f'title="{w.s(link["name"] + ": " + link["url"])}">{w.s(link["title"])}</a>'
                            for link in project.web_links())
        except Exception as e:
            return 'Error: ' + w.s(str(e))

    def git_action_buttons(self, project: 'GitRepo') -> str:
        name = html.escape(json.dumps(project.type), quote=True)
        return ' '.join(
            f'<button type="button" title="{html.escape(hint, quote=True)}" '
            f'onclick="projects_git_action(this, {name}, \'{op}\')">{title}</button>'
            for op, title, hint in self.GIT_ACTIONS)

    def part_gitactions(self, p: base.Page, params={}):
        """
        Git-Projekte mit Status und Remotes, Aktionen up (commit & push) und pull je Projekt
        """
        projects = [project for project in self.expand_projects() if isinstance(project, GitRepo)]
        remotes = {project.type: self.project_remotes(project) for project in projects}
        web = {project.type: self.project_web(project) for project in projects}
        p.grid(projects, [
            w.Grid.col('type', "Name", value=lambda pr: w.a(pr.type, f'?type={pr.type}'), html=True),
            w.Grid.col('status', '<span title="↑ ahead, ↓ behind, * geänderte Dateien, ✓ sauber">Status</span>', sortable=False,
                       value=lambda pr: '...', td={'_class': 'project_status', 'data_name': lambda pr: pr.type}),
            w.Grid.col('remotes', "Remotes", html=True, sortable=False, value=lambda pr: remotes[pr.type]),
            w.Grid.col('web', "Web", html=True, sortable=False, value=lambda pr: web[pr.type]),
            w.Grid.col('path', "Path"),
            w.Grid.col('actions', "Actions", html=True, sortable=False, value=self.git_action_buttons),
            w.Grid.col('result', '<span title="Ergebnis der letzten Aktion, Tooltip: Ausgabe von git">Result</span>',
                       sortable=False, value=lambda pr: '', td={'_class': 'git_result'}),
        ], params, search=lambda pr: pr.type + ' ' + pr.path + ' ' + remotes[pr.type], count="{shown} / {total} Repos")
        p.js_ready(self.js_project_status() + """
            window.projects_git_action = function(btn, name, op) {
                const row = btn.closest('tr');
                const out = row.querySelector('td.git_result');
                row.querySelectorAll('button').forEach((b) => b.disabled = true);
                out.textContent = op + ' ...';
                out.title = '';
                fetch(base_url(), post({type: name, op: op})).then((response) => response.json()).then((res) => {
                    out.textContent = res.success ? (op + ': ok') : ('Error: ' + res.message);
                    out.style.color = res.success ? 'green' : 'red';
                    out.title = (res.lines || []).join('\\n');
                    const td = row.querySelector('td.project_status');
   