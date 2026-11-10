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


SOURCE_EXTS = ['java', 'py', 'html', 'css', 'js', 'ts', 'php', 'rt', 'kt', 'h', 'c', 'cpp']

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
                    if (td) { projects_load_status(td); }
                }).catch((e) => {
                    out.textContent = 'Error: ' + e;
                    out.style.color = 'red';
                }).finally(() => {
                    row.querySelectorAll('button').forEach((b) => b.disabled = false);
                });
            };
        """)
        p.div(w.a("Projects", self.link('index')))

    # werden immer uebersprungen (Interna der Versionsverwaltung)
    SEARCH_ALWAYS_SKIP_DIRS = {'.git', '.svn', '.hg'}
    # werden zusaetzlich zum exclude_filter der Projekte uebersprungen, wenn der Filter aktiv ist
    SEARCH_SKIP_DIRS = {*SEARCH_ALWAYS_SKIP_DIRS, 'node_modules', '__pycache__', '.pio', '.idea', '.gradle',
                        'build', 'dist', 'venv', '.venv', 'target', '.tox', '.mypy_cache'}
    SEARCH_MAX_FILE_BYTES = 1024 * 1024
    SEARCH_MAX_RESULTS = 500

    def search_files(self, projects: List['ProjectDir'], q: str, content: bool = False, use_exclude_filter: bool = True):
        """
        Durchsucht alle Projekte nach Dateinamen oder Inhalten (Gross-/Kleinschreibung egal)

        Binaerdateien und Dateien ueber SEARCH_MAX_FILE_BYTES werden uebersprungen.

        :param use_exclude_filter: Pfade aus dem exclude_filter des Projekts und SEARCH_SKIP_DIRS ueberspringen,
                                   sonst nur SEARCH_ALWAYS_SKIP_DIRS
        :return: Generator von (project, rel_path, line_nr, line), line_nr/line sind bei der Dateinamensuche None
        """
        q = q.lower()
        skip_dirs = self.SEARCH_SKIP_DIRS if use_exclude_filter else self.SEARCH_ALWAYS_SKIP_DIRS
        for project in projects:
            is_excluded = getattr(project, 'is_excluded', None)
            if is_excluded is None or not use_exclude_filter:
                is_excluded = lambda rel_path: False
            for root, dirs, files in os.walk(project.path):
                rel_root = os.path.relpath(root, project.path).replace('\\', '/')
                rel_root = '' if rel_root == '.' else rel_root + '/'
                dirs[:] = [d for d in dirs if d not in skip_dirs and not is_excluded(rel_root + d)]
                for f in files:
                    path = os.path.join(root, f)
                    rel = os.path.relpath(path, project.path).replace('\\', '/')
                    if is_excluded(rel):
                        continue
                    if not content:
                        if q in rel.lower():
                            yield project, rel, None, None
                        continue
                    try:
                        if os.path.getsize(path) > self.SEARCH_MAX_FILE_BYTES:
                            continue
                        with open(path, 'rb') as fh:
                            if b'\0' in fh.read(1024):
                                continue  # Binaerdatei
                            fh.seek(0)
                            for nr, line in enumerate(fh, 1):
                                text = line.decode('utf-8', errors='replace')
                                if q in text.lower():
                                    yield project, rel, nr, text.strip()
                    except OSError:
                        pass

    def part_search(self, p: base.Page, params={}):
        """
        Suche in allen Projekten, nach Dateinamen (mode=name) oder Inhalten (mode=content)
        """
        q = params.get('q', '')
        mode = params.get('mode', 'name')
        use_exclude_filter = _exclude_filter_param(params)
        with p.t('form'):
            p(w.hidden('type', self.type) + w.hidden(self.type, 'search'))
            p.form_input('q', "Search", value=w.s(q))
            for value, title in [('name', "Dateinamen"), ('content', "Inhalte")]:
                checked = ' checked' if value == mode else ''
                p(f'<label><input type="radio" name="mode" value="{value}"{checked}> {title}</label> ')
            p(_exclude_filter_checkbox(use_exclude_filter))
            p.input('submit', value="Search", type='submit')
        if q.strip() != '':
            projects = self.expand_projects()
            count = 0
            current = None
            for project, rel, nr, line in self.search_files(projects, q.strip(), mode == 'content', use_exclude_filter):
                if count >= self.SEARCH_MAX_RESULTS:
                    p.div(f"Mehr als {self.SEARCH_MAX_RESULTS} Treffer, Suche abgebrochen.")
                    break
                if project is not current:
                    current = project
                    p.h3(w.a(w.s(project.type), f'?type={project.type}'))
                link = w.a(w.s(rel), project.link(project.part_file, path=rel))
                if nr is None:
                    p.div(link)
                else:
                    p.div(f'{link}:{nr} <code>{w.s(line[:200])}</code>')
                count += 1
            if count == 0:
                p.div("Keine Treffer.")
        p.div(w.a("Projects", self.link('index')))

    @staticmethod
    def project_depends(project: 'ProjectDir') -> List[str]:
        """
        :return: Abhaengigkeiten aus props['depends'] (kommagetrennt, z.B. in der nweb.ini: depends = nwebclient, nxbot)
        """
        depends = getattr(project, 'props', {}).get('depends', '')
        return [d.strip() for d in str(depends).split(',') if d.strip() != '']

    def dependency_graph(self, projects: List['ProjectDir']) -> dict:
        """
        :return: {'nodes': {name: {'depends': [...], 'level': int, 'external': bool}}, 'cycles': [(a, b)]}
                 level 0 = ohne Abhaengigkeiten, sonst 1 + hoechstes level der Abhaengigkeiten
        """
        nodes = {}
        for project in projects:
            nodes[project.type] = {'depends': self.project_depends(project), 'external': False}
        for name in [d for n in list(nodes.values()) for d in n['depends']]:
            if name not in nodes:
                nodes[name] = {'depends': [], 'external': True}  # nicht in nweb.json:repos
        cycles = []
        levels = {}

        def level(name, visiting):
            if name in levels:
                return levels[name]
            visiting.add(name)
            res = 0
            for dep in nodes[name]['depends']:
                if dep in visiting:
                    cycles.append((name, dep))  # Zyklus, Kante wird nicht fuer das Level beruecksichtigt
                    continue
                res = max(res, level(dep, visiting) + 1)
            visiting.discard(name)
            levels[name] = res
            return res

        for name in nodes:
            nodes[name]['level'] = level(name, set())
        return {'nodes': nodes, 'cycles': cycles}

    @staticmethod
    def _graph_components(nodes: dict, names: List[str]) -> List[List[str]]:
        """
        :return: zusammenhaengende Gruppen (Kanten ungerichtet), groesste zuerst
        """
        neighbours = {name: set() for name in names}
        for name in names:
            for dep in nodes[name]['depends']:
                if dep in neighbours:
                    neighbours[name].add(dep)
                    neighbours[dep].add(name)
        res, seen = [], set()
        for name in sorted(names, key=str.lower):
            if name in seen:
                continue
            component, todo = [], [name]
            seen.add(name)
            while todo:
                n = todo.pop()
                component.append(n)
                for m in neighbours[n] - seen:
                    seen.add(m)
                    todo.append(m)
            res.append(component)
        return sorted(res, key=len, reverse=True)

    def part_graph(self, p: base.Page, params={}):
        """
        Repos mit ihren Abhaengigkeiten (props['depends']). Pfeile zeigen auf die Abhaengigkeit,
        Projekte ohne Abhaengigkeiten stehen unten, Repos ohne jede Verbindung werden darunter aufgelistet.
        """
        graph = self.dependency_graph(self.expand_projects())
        nodes = graph['nodes']
        used = {d for n in nodes.values() for d in n['depends']}
        connected = [name for name, n in nodes.items() if n['depends'] or name in used]
        isolated = sorted(name for name in nodes if name not in connected)
        p.h1("Projects: Graph")
        if len(connected) == 0:
            p.div("Keine Abhaengigkeiten definiert. In der nweb.ini eines Repos z.B.: depends = nwebclient, nxbot")
        else:
            node_w, node_h, gap_x, row_h, margin = 140, 32, 30, 90, 10
            max_level = max(nodes[n]['level'] for n in connected)
            x_of = {}
            offset = margin
            # zusammenhaengende Gruppen nebeneinander, damit sich unabhaengige Repos nicht vermischen
            for component in self._graph_components(nodes, connected):
                rows = {}
                for name in component:
                    rows.setdefault(nodes[name]['level'], []).append(name)
                for lv in sorted(rows):
                    row = rows[lv]
                    if lv == 0:
                        row.sort(key=str.lower)
                    else:
                        # Mittelwert der x-Positionen der Abhaengigkeiten, verringert Ueberschneidungen
                        def center(name):
                            xs = [x_of[d] for d in nodes[name]['depends'] if d in x_of]
                            return (sum(xs) / len(xs) if xs else 0, name.lower())
                        row.sort(key=center)
                    for i, name in enumerate(row):
                        x_of[name] = offset + i * (node_w + gap_x)
                offset += max(len(r) for r in rows.values()) * (node_w + gap_x) + gap_x
            width = offset - gap_x + margin
            height = (max_level + 1) * row_h + 2 * margin
            y_of = {name: margin + (max_level - nodes[name]['level']) * row_h for name in connected}
            cycles = set(graph['cycles'])
            svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
                   f'viewBox="0 0 {width} {height}" font-family="sans-serif" font-size="12">',
                   '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
                   'orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="currentColor"/></marker></defs>']
            for name in connected:
                for dep in nodes[name]['depends']:
                    x1, y1 = x_of[name] + node_w / 2, y_of[name] + node_h
                    x2, y2 = x_of[dep] + node_w / 2, y_of[dep]
                    if y2 <= y1:  # Zyklus: Abhaengigkeit liegt nicht darunter, versetzt neben der Gegenkante
                        y1, y2 = y_of[name], y_of[dep] + node_h
                        x1, x2 = x1 + 12, x2 + 12
                    color = 'red' if (name, dep) in cycles else 'currentColor'
                    if abs(y2 - y1) > row_h:
                        # ueber mehrere Ebenen: durch die Luecke rechts neben den Knoten fuehren, nicht durch Knoten
                        xm = max(x_of[name], x_of[dep]) + node_w + gap_x + 10  # Kurve erreicht ca. 3/4 davon
                        d = f'M{x1} {y1} C{xm} {y1 + row_h / 2} {xm} {y2 - row_h / 2} {x2} {y2}'
                    else:
                        my = (y1 + y2) / 2
                        d = f'M{x1} {y1} C{x1} {my} {x2} {my} {x2} {y2}'
                    svg.append(f'<path d="{d}" fill="none" stroke="{color}" '
                               f'stroke-opacity="0.6" stroke-width="1.5" marker-end="url(#arrow)"/>')
            for name in connected:
                n = nodes[name]
                x, y = x_of[name], y_of[name]
                label = name if len(name) <= 18 else name[:17] + '…'
                title = name + (': ' + ', '.join(n['depends']) if n['depends'] else '')
                dash = ' stroke-dasharray="4 3" opacity="0.6"' if n['external'] else ''
                rect = (f'<rect x="{x}" y="{y}" width="{node_w}" height="{node_h}" rx="6" fill="none" '
                        f'stroke="currentColor"{dash}/>'
                        f'<text x="{x + node_w / 2}" y="{y + node_h / 2 + 4}" text-anchor="middle" fill="currentColor">'
                        f'{w.s(label)}</text><title>{w.s(title)}</title>')
                if n['external']:
                    svg.append(f'<g>{rect}</g>')
                else:
                    svg.append(f'<a href="?type={w.s(name)}"><g>{rect}</g></a>')
            svg.append('</svg>')
            # hpcc-zoom: Mausrad zoomt, Ziehen verschiebt; Startzoom passt breite Graphen ungefaehr auf die Seitenbreite
            scale = round(min(1.0, 1100 / width), 2)
            view_h = max(300, min(int(height * scale) + 40, 800))
            p.script('/static/js/hpcc/wc-layout.js')
            p(f'<hpcc-zoom x="0" y="0" scale="{scale}" style="display: block; width: 100%; height: {view_h}px; '
              f'border: 1px solid rgba(128, 128, 128, 0.3);">' + ''.join(svg) + '</hpcc-zoom>')
            p.div("Mausrad: zoomen, Ziehen: verschieben. "
                  "Pfeile zeigen auf die Abhaengigkeit. Gestrichelt: nicht in nweb.json:repos. Rot: zyklische Abhaengigkeit.",
                  style='font-size: 0.85em; opacity: 0.7;')
        if len(isolated) > 0:
            p.h3("Ohne Abhaengigkeiten")
            p.div(', '.join(w.a(w.s(name), f'?type={w.s(name)}') for name in isolated))
        p.div(w.a("Projects", self.link('index')))

    def part_system(self, p: base.Page, params={}):
        p.prop("~/dev", (Path.home() / 'dev').exists())
        p.prop("~/repos", (Path.home() / 'repos').exists())


class GitlabCi(r.BaseJobExecutor):
    """
    Liest eine .gitlab-ci.yml und fuehrt die script-Befehle ihrer Jobs lokal aus.
    """

    NON_JOB_KEYS = ['image', 'stages']

    def __init__(self, file='.gitlab-ci.yml'):
        super().__init__()
        self.type = os.path.basename(file.replace('.gitlab-ci.yml', ''))
        from yaml import load, dump
        try:
            from yaml import CLoader as Loader, CDumper as Dumper
        except ImportError:
            from yaml import Loader, Dumper
        self.file = file
        with open(file, 'r') as f:
            self.data = load(f, Loader=Loader)

    def on_line(self, txt):
        self.info(txt)

    def execute_ci(self, verbose=False):
        for key in self.data.keys():
            if key not in self.NON_JOB_KEYS:
                self.execute_job(key, verbose)

    def execute_job(self, name, verbose=False):
        if name not in self.data:
            self.error("Job")
        if verbose:
            print("Running " + name)
        job = self.data[name]
        for cmd in job.get('script', []):
            r.ProcessExecutor(cmd, on_line=self.on_line).waitForEnd()


_compose_cmd = None


def compose_cmd() -> List[str]:
    """
    Befehl fuer Docker Compose: das Plugin ``docker compose``, sonst das eigenstaendige ``docker-compose``

    :return: z.B. ['docker', 'compose'] (wird nach dem ersten Aufruf zwischengespeichert)
    """
    global _compose_cmd
    if _compose_cmd is None:
        _compose_cmd = ['docker', 'compose']
        for cmd in [['docker', 'compose'], ['docker-compose']]:
            try:
                if subprocess.run([*cmd, 'version'], capture_output=True, timeout=10).returncode == 0:
                    _compose_cmd = cmd
                    break
            except (OSError, subprocess.TimeoutExpired):
                pass
    return _compose_cmd


def compose_shell(args: str) -> str:
    """
    :return: Compose-Befehl als String fuer ProcessExecutor, z.B. ``docker compose up -d``
    """
    return ' '.join(compose_cmd()) + ' ' + args


# Dateinamen, die docker compose ohne -f findet (in dieser Reihenfolge)
COMPOSE_FILES = ['compose.yaml', 'compose.yml', 'docker-compose.yaml', 'docker-compose.yml']


def compose_file(path: str) -> Optional[str]:
    """
    :return: Pfad der Compose-Datei im Verzeichnis oder None
    """
    for name in COMPOSE_FILES:
        f = os.path.join(path, name)
        if os.path.isfile(f):
            return f
    return None


class DockerComposeHelper:
    def __init__(self, path):
        from yaml import load
        try:
            from yaml import CLoader as Loader
        except ImportError:
            from yaml import Loader
        self.path = path
        self.data = {}
        self.services = {}
        self.compose_file = compose_file(path)
        if self.compose_file is not None:
            with open(self.compose_file, 'r') as f:
                self.data = load(f, Loader=Loader) or {}
            self.services = self.data.get('services') or {}

    def compose(self, *args, timeout=30) -> subprocess.CompletedProcess:
        """
        Fuehrt ``docker compose <args>`` (bzw. ``docker-compose``) im Projektverzeichnis aus und wartet auf das Ende
        """
        return subprocess.run([*compose_cmd(), *args], cwd=self.path, capture_output=True, text=True,
                              timeout=timeout)

    def is_running(self) -> bool:
        """
        True, wenn mindestens ein Container des Compose-Projekts laeuft
        """
        try:
            res = self.compose('ps', '--services', '--filter', 'status=running')
            return res.returncode == 0 and res.stdout.strip() != ''
        except (OSError, subprocess.TimeoutExpired):
            return False

    def start(self):
        self.process = r.ProcessExecutor(compose_shell('up -d'), cwd=self.path)

    def stop(self):
        self.process = r.ProcessExecutor(compose_shell('down'), cwd=self.path)

    def names(self) -> list[str]:
        return list(self.services.keys())

    def ports(self) -> list[str]:
        """
        :return: Host-Ports aller Services, z.B. ['8080'] fuer "8080:80" oder "127.0.0.1:8080:80/tcp"
        """
        results = []
        for container in self.services.values():
            for port in (container or {}).get('ports', []) or []:
                if isinstance(port, dict):
                    if port.get('published') is not None:
                        results.append(str(port['published']))
                else:
                    parts = str(port).split('/')[0].split(':')
                    results.append(parts[-2] if len(parts) >= 2 else parts[0])
        return results

    def logs(self, tail=200) -> str:
        try:
            res = self.compose('logs', '--no-color', '--tail', str(tail))
            return res.stdout + res.stderr
        except (OSError, subprocess.TimeoutExpired) as e:
            return f"Error: {e}"


class PackageJson:
    def __init__(self, path: str):
        if not path.endswith('package.json'):
            path = os.path.join(path, 'package.json')
        self.path = path
        self.data = json.load(path)

    def scripts(self):
        return self.data.get('scripts', {})


class ProjectDir(r.BaseJobExecutor):
    """
    Projektverzeichnis mit Dateiansicht, Suche, TODO-Liste, Metriken, README und Aktionen fuer Git, Docker, Docker Compose, npm und Shell-Skripte.
    """

    TAGS = []

    # Muster fuer Verzeichnisse/Dateien mit redundanten Inhalten, die bei Suchen ignoriert werden (fnmatch).
    # Ohne "/" wird jeder Pfadbestandteil geprueft (z.B. "build", "*.egg-info"), mit "/" der relative Pfad (z.B. "docs/_build").
    # Unterklassen erweitern die Liste, in der nweb.ini z.B. mit: exclude_filter = docs/_build, *.min.js
    EXCLUDE_FILTER = ['.git', '.svn', '.hg', '.idea', 'node_modules']

    # Anzahl der Zeilen vor und nach einem TODO in der TODO-Ansicht
    TASK_CONTEXT_LINES = 3

    def __init__(self, path='', props: dict = None):
        super().__init__(path.strip('/').split('/')[-1])
        self.path = path
        self.props = {} if props is None else dict(props)
        self.exclude_filter = list(self.EXCLUDE_FILTER)
        self.read_properties()

    def read_properties(self):
        """
        Liest nweb.ini nach self.props

        Schluessel ohne Section landen direkt in self.props, Sections als dict:
        ``{'key': 'v', 'section': {'key': 'v'}}``
        """
        if self.exists('nweb.ini'):
            import configparser
            root = '__root__'
            cp = configparser.ConfigParser(interpolation=None)
            cp.optionxform = str  # Gross-/Kleinschreibung der Keys beibehalten
            try:
                cp.read_string(f'[{root}]\n' + u.file_get_text(os.path.join(self.path, 'nweb.ini')))
            except configparser.Error as e:
                self.error(f"Invalid nweb.ini: {e}")
                return self.props
            for section in cp.sections():
                if section == root:
                    self.props.update(cp[section])
                else:
                    self.props[section] = {**self.props.get(section, {}), **cp[section]}

        if 'tags' in self.props:
            self.TAGS = [*self.TAGS, *[t.strip() for t in str(self.props['tags']).split(',') if t.strip() != '']]
        if 'exclude_filter' in self.props:
            for pattern in str(self.props['exclude_filter']).split(','):
                pattern = pattern.strip().strip('/')
                if pattern != '' and pattern not in self.exclude_filter:
                    self.exclude_filter.append(pattern)
        return self.props

    def is_excluded(self, rel_path: str) -> bool:
        """
        True, wenn der Pfad (relativ zum Projekt) durch exclude_filter ausgeschlossen ist
        """
        rel_path = rel_path.replace('\\', '/').strip('/')
        parts = rel_path.split('/')
        for pattern in self.exclude_filter:
            if '/' in pattern:
                if fnmatch(rel_path, pattern) or fnmatch(rel_path, pattern + '/*'):
                    return True
            elif any(fnmatch(part, pattern) for part in parts):
                return True
        return False

    def run(self):
        if self.is_python():
            return self.run_python()
        elif self.is_java():
            return self.run_java()
        elif self.is_docker():
            pass
        elif self.is_docker_compose():
            self.run_compose()
        else:
            pass

    def run_java(self):
        pass

    def run_python(self):
        pass

    def is_python(self):
        return (self.exists('setup.py') or self.exists('app.py') or
                self.exists('__main__.py') or self.exists('__init__.py') or
                self.exists('pyproject.toml'))

    def product_type(self):
        if 'product_type' in self.props:
            return self.props['product_type']
        if self.is_android():
            return 'android-app'
        if 'lib' in self.TAGS:
            return 'lib'
        if self.exists('library.json'):
            return 'arduino-library'
        if self.exists('inc.php'):
            return 'webapp'
        return 'software'

    def is_java(self):
        return self.exists('pom.xml')

    def is_android(self):
        return (self.exists('app') and
            (self.exists('build.gradle.kts') or self.exists('build.gradle'))
        )

    def is_docker(self):
        return self.exists('Dockerfile')

    def is_docker_compose(self):
        return compose_file(self.path) is not None

    def run_compose(self):
        r.ProcessExecutor(compose_shell('up -d'), cwd=self.path)

    def execute(self, data):
        if 'readme' in data:
            return self.success('Readme', value=self.readme_text())
        return super().execute(data)

    def exists(self, path):
        return os.path.exists(os.path.join(self.path, path))

    def readme_text(self):
        try:
            return u.file_get_contents(self.path + '/README.md').decode('utf-8')
        except:
            return ''

    def is_file_versionized(self, rel_path: str) -> bool:
        """
        True, wenn die Datei nicht durch .gitignore ausgeschlossen wird.
        Unterstützt:
        - Kommentare (#)
        - Leerzeilen
        - Wildcards (*, ?, **)
        - Negation (!)
        """
        project_dir = Path(self.path)
        ignore_file = project_dir / ".gitignore"
        if not ignore_file.exists():
            return True
        rel_path = rel_path.replace("\\", "/")
        ignored = False
        for line in ignore_file.read_text(encoding="utf-8").splitlines():
            pattern = line.strip()
            if not pattern or pattern.startswith("#"):
                continue
            negate = pattern.startswith("!")
            if negate:
                pattern = pattern[1:]
            match = (
                    fnmatch(rel_path, pattern)
                    or fnmatch(Path(rel_path).name, pattern)
            )
            if match:
                ignored = not negate
        return not ignored

    def link_file(self, rel_path, **kwargs):
        return w.a(rel_path, self.link(self.part_file, path=rel_path, **kwargs))

    def part_files(self, p: base.Page, params={}):
        for f in self.glob():
            p.div(self.link_file(self.to_rel_path(f)))

    def glob(self, path='/**', recursive=True, use_exclude_filter=False):
        """
        :param use_exclude_filter: Dateien aus exclude_filter (z.B. build-Verzeichnisse) weglassen
        """
        res = glob.glob(self.path + path, recursive=recursive)
        if use_exclude_filter:
            res = [f for f in res if not self.is_excluded(self.to_rel_path(f))]
        return res

    def get_lines_of_code(self):
        line_count = 0
        for f in self.glob(use_exclude_filter=True):
            if self.is_file_versionized(self.to_rel_path(f)) and is_source_file(f):
                line_count += len(u.file_get_lines(f))
        return line_count

    def part_metrics(self, p: base.Page, params={}):
        p.prop("Lines of Code", self.get_lines_of_code())

    def part_file(self, p: base.Page, params={}):
        # TODO Syntaxhighlighting etc.
        apath = self.path + '/' + params['path']
        p.h2(params['path'])
        p.pre(u.file_get_text(apath))

    def part_search(self, p: base.Page, params={}):
        use_exclude_filter = _exclude_filter_param(params)
        with p.t('form'):
            p.form_input('q', "Search", value=w.s(params.get('q', '')))
            p(w.hidden('type', self.type)+w.hidden(self.type, 'search'))
            p(_exclude_filter_checkbox(use_exclude_filter))
            p.input('submit', value="Search", type='submit')
        if 'q' in params:
            for f in self.glob(use_exclude_filter=use_exclude_filter):
                try:
                    if os.path.isfile(f) and params['q'] in u.file_get_text(f):
                        p.div(self.link_file(self.to_rel_path(f), q=params['q']))
                except Exception as e:
                    print(e)

    def to_rel_path(self, path:str):
        res = path.replace(self.path, '')
        if res.startswith('/'):
            res = res[1:]
        return res

    def part_index(self, p: base.Page, params={}):
        with p.section():
            p.prop("Path", self.path)
            p.prop("Exclude", ', '.join(self.exclude_filter))
            p.ul([
                w.a("Files", self.link('files')),
                w.a("ToDo", self.link('tasks')),
                w.a("ReadMe", self.link('readme')),
                w.a("CodeServer", self.link('codeserver')),
                w.a("Search in Files", self.link(self.part_search)),
                w.a("Metrics", self.link(self.part_metrics)),
            ])
        if self.exists('.git'):
            self.show_git(p, params)
        if self.exists('.gitlab-ci.yml'):
            self.show_gitlabci(p, params)
        if self.exists('nweb.ini') or self.exists('.nweb.ini'):
            self.show_nweb(p, params)
        if self.is_docker():
            self.show_docker(p, params)
        if self.is_docker_compose():
            self.show_compose(p, params)
        if self.exists('package.json'):
            self.show_npm(p, params)
        if self.exists('run.sh'):
            self.show_sh(p, params)
        with p.section():
            p.markdown(self.readme_text())
        with p.section(h="Result"):
            p.pre('', id='result')

    def show_gitlabci(self, p: base.Page, params={}):
        with p.section(h="Gitlab CI"):
            pass

    def show_nweb(self, p: base.Page, params={}):
        with p.section(h="nweb"):
            p("nweb.ini")

    def show_docker(self, p: base.Page, params={}):
        with p.section(h="Docker"):
            if self.exists('Dockerfile'):
                p.pre('docker build -t' + self.type + ' .')

    def show_sh(self, p: base.Page, params={}):
        if self.exists('run.sh'):
            p(self.action_btn(dict(title="run.sh", type=self.type, op='npm', script='sh run.sh')))
        if self.exists('stop.sh'):
            p(self.action_btn(dict(title="stop.sh", type=self.type, op='npm', script='sh stop.sh')))
        p(self.action_btn(dict(title="du -h", type=self.type, op='npm', script='du -h .')))

    def show_npm(self, p: base.Page, params={}):
        with p.section(h="npm package.json"):
            p.div('{ "scripts": {} }')
            npm = PackageJson(self.path)
            for k, v in npm.scripts().items():
                p(self.action_btn(dict(title=k, type=self.type, op='npm', script=v)))
            p.pre('', id='result')

    def execute_npm(self, data):
        lines = r.ProcessExecutor(data['script'], cwd=self.path).waitForEnd().lines()
        return self.success(lines=lines)

    def show_git(self, p: base.Page, params={}):
        with p.section():
            p(self.action_btn(dict(type=self.type, op='push', title="Push")))
            p(self.action_btn(dict(type=self.type, op='pull', title="Pull")))
            p(self.action_btn(dict(type=self.type, op='status', title="Status")))

    def execute_push(self, data):
        commit = r.ProcessExecutor('git commit -am sc', cwd=self.path).waitForEnd().lines()
        push = r.ProcessExecutor('git push', cwd=self.path).waitForEnd().lines()
        return self.success(lines=[*commit, *push])

    def execute_pull(self, data):
        proc = r.ProcessExecutor('git pull', cwd=self.path).waitForEnd()
        exit_code = proc.p.returncode
        if exit_code != 0:
            return self.fail('git pull failed', lines=proc.lines(), exit_code=exit_code)
        return self.success(lines=proc.lines(), exit_code=exit_code)

    def execute_status(self, data):
        lines = r.ProcessExecutor('git status', cwd=self.path).waitForEnd().lines()
        return self.success(lines=lines)

    def show_compose(self, p: base.Page, params={}):
        c = DockerComposeHelper(self.path)
        with p.section(h="Docker Compose"):
            p.prop("Running", c.is_running())
            if isinstance(self, DockerCompose):
                p(self.action_btn(dict(type=self.type, op='start', title="Start")))
                p(self.action_btn(dict(type=self.type, op='stop', title="Stop")))
            p.right(w.a("Logs", self.link(self.part_logs)))
            p.prop("File", os.path.basename(c.compose_file))
            p.prop("Ports", ', '.join(c.ports()))
            p.pre(w.s(u.file_get_text(c.compose_file)))

    def part_logs(self, p: base.Page, params={}):
        c = DockerComposeHelper(self.path)
        p.pre(w.s(c.logs()))

    def part_tasks(self, p: base.Page, params={}):
        try:
            context = max(0, int(params.get('context', self.TASK_CONTEXT_LINES)))
        except ValueError:
            context = self.TASK_CONTEXT_LINES
        for f in self.glob(use_exclude_filter=True):
            if os.path.isfile(f) and is_file_cat(f, True, True):
                self.tasks_for_file(f, p, context)

    def tasks_for_file(self, f, p: b.Page, context: int = TASK_CONTEXT_LINES):
        """
        Zeigt alle TODOs einer Datei mit `context` Zeilen davor und danach
        """
        try:
            lines = u.file_get_text(f).splitlines()
        except Exception as e:
            self.error(f"tasks_for_file {f}: {e}")
            return
        rel_path = self.to_rel_path(f)
        for i, line in enumerate(lines):
            if 'TO'+'DO' in line:
                start, end = max(0, i - context), min(len(lines), i + context + 1)
                rows = []
                for nr in range(start, end):
                    text = f'{nr + 1:5}  {w.s(lines[nr])}'
                    rows.append(f'<b>{text}</b>' if nr == i else text)
                p.div(self.link_file(rel_path) + f":{i + 1}")
                p.pre('\n'.join(rows), style='margin-top: 0;')

    def page_readme(self, params={}):
        p = b.Page(owner=self)
        p.markdown(self.readme_text())
        return p.nxui()

    def part_codeserver(self, p: base.Page, params={}):
        p.h1("CodeServer")
        from nxbot.linux import Docker
        d = Docker()
        port = int(params.get('port', 3000))
        name = 'dev' + self.type
        if not d.is_running(name):
            # TODO option für docker
            d.run('gitpod/openvscode-server', name, ports={'3000/tcp': port}, volumes={self.path: '/home/workspace:cached'})
            time.sleep(5)
        p.a("Visual Studio Code", f'http://127.0.0.1:{port}/')
        # docker run -it --init -p 3000:3000 -v "$(pwd):/home/workspace:cached" gitpod/openvscode-server


class DockerCompose(ProjectDir):
    """
    Projektverzeichnis mit Compose-Datei, dessen Docker-Compose-Dienste gestartet und gestoppt werden koennen.
    """
    def __init__(self, path):
        super().__init__(path)
        self.compose = DockerComposeHelper(path)

    def start(self):
        self.compose.start()
        return self.success()

    def stop(self):
        self.compose.stop()
        return self.success()

    def execute_start(self, data):
        return self.start()

    def execute_stop(self, data):
        return self.stop()


class BlameEntry:

    def __init__(self, array, file=''):
        self.commit = array[0]
        self.lines = array[1]
        self.file = file

    def __len__(self):
        return len(self.lines)

    def author(self):
        return self.commit.author.email

    def text(self):
        return "\n".join(self.lines)


class GitRepo(ProjectDir):
    """
    @see GitBot
    """

    MODULES = ['GitPython']

    def __init__(self, path):
        if path.startswith('https://') or path.startswith('git@'):
            path = self.clone_repo(path)
        super().__init__(path)
        from git import Repo
        self.repo = Repo(path)
        self.define_sig(dev.PStr('op', 'status_short')).set_description('Kurzstatus des Repos (Branch, Commits voraus, geänderte Dateien)').r(success=bool, message=str, value=str,
                                                          hint=str, up_to_date=bool)
        self.define_sig(dev.PInt('status', 1)).set_description('Status des Arbeitsverzeichnisses (ohne fetch), message ist die Commit-Message').r(
            success=bool, message=str, branch=str, commit=str, dirty=bool, staged=list, modified=list,
            untracked=list, tracking=str, ahead=int, behind=int)

    def clone_repo(self, url):
        name = url.split('/')[-1].replace('.git', '')
        if (Path.home() / 'repos').exists():
            p = Path.home() / 'repos'
        else:
            p = Path.home()
        if not (p / name).exists():
            cmd = f'git clone {url} {name}'
            r.ProcessExecutor(cmd=cmd).waitForEnd()
        return str(p / name)

    def status(self) -> dict:
        """
        Status des Arbeitsverzeichnisses (ohne fetch, ahead/behind bezieht sich auf den lokalen Stand des Remotes)

        :return: dict mit branch, commit, dirty, staged, modified, untracked, tracking, ahead, behind
        """
        repo = self.repo
        res = {
            'branch': None, 'commit': None, 'message': None,
            'dirty': repo.is_dirty(untracked_files=True),
            'staged': [], 'modified': [], 'untracked': repo.untracked_files,
            'tracking': None, 'ahead': 0, 'behind': 0,
        }
        res['modified'] = [d.a_path for d in repo.index.diff(None)]
        if not repo.head.is_valid():
            # Repo ohne Commits
            res['branch'] = repo.head.reference.name
            res['staged'] = [e[0] for e in repo.index.entries.keys()]
            return res
        commit = repo.head.commit
        res['commit'] = commit.hexsha
        res['message'] = commit.summary
        res['staged'] = [d.a_path for d in repo.index.diff('HEAD')]
        if repo.head.is_detached:
            return res
        branch = repo.active_branch
        res['branch'] = branch.name
        tracking = branch.tracking_branch()
        if tracking is not None and tracking.is_valid():
            res['tracking'] = tracking.name
            res['ahead'] = sum(1 for _ in repo.iter_commits(f'{tracking.name}..{branch.name}'))
            res['behind'] = sum(1 for _ in repo.iter_commits(f'{branch.name}..{tracking.name}'))
        return res

    def status_short(self, s: dict = None) -> str:
        """
        Kurzform des Status, z.B. ``main ↑1 ↓2 *3``

        ↑ ahead, ↓ behind, * Anzahl geaenderter Dateien (staged, modified, untracked), ✓ sauber

        :param s: Ergebnis von status(), wird sonst ermittelt
        """
        if s is None:
            s = self.status()
        res = s['branch'] if s['branch'] is not None else (s['commit'] or '-')[:8]
        if s['ahead'] > 0:
            res += f" ↑{s['ahead']}"
        if s['behind'] > 0:
            res += f" ↓{s['behind']}"
        changes = len(set(s['staged'] + s['modified'] + s['untracked']))
        res += f' *{changes}' if changes > 0 else ' ✓'
        return res

    def is_up_to_date(self, s: dict = None) -> bool:
        """
        True, wenn keine lokalen Aenderungen vorliegen und der Branch gleichauf mit seinem Tracking-Branch ist

        :param s: Ergebnis von status(), wird sonst ermittelt
        """
        if s is None:
            s = self.status()
        return s['tracking'] is not None and s['ahead'] == 0 and s['behind'] == 0 and not s['dirty']

    def status_hint(self, s: dict = None) -> str:
        """
        Ausfuehrliche Beschreibung des Status, z.B. als Tooltip zu status_short()

        :param s: Ergebnis von status(), wird sonst ermittelt
        """
        if s is None:
            s = self.status()
        lines = [f"Branch: {s['branch'] if s['branch'] is not None else '(detached)'}",
                 f"Commit: {(s['commit'] or '-')[:8]} {s['message'] or ''}".rstrip()]
        if s['tracking'] is not None:
            lines.append(f"Remote: {s['tracking']} (↑{s['ahead']} ahead, ↓{s['behind']} behind)")
        else:
            lines.append("Remote: kein Tracking-Branch")
        lines.append(f"Staged: {len(s['staged'])}, Modified: {len(s['modified'])}, Untracked: {len(s['untracked'])}")
        return '\n'.join(lines)

    def execute_status_short(self, data={}):
        """
        Kurzstatus, z.B. ``{'value': 'main ↑1 *2', 'hint': 'Branch: main ...', 'up_to_date': False}``
        """
        s = self.status()
        return self.success('Status', value=self.status_short(s), hint=self.status_hint(s),
                            up_to_date=self.is_up_to_date(s))

    def execute(self, data):
        if 'status' in data:
            return self.success('Status', **self.status())
        return super().execute(data)

    @r.op("Commit aller Aenderungen an versionierten Dateien (git commit -a) und git push",
          success={'lines': list, 'exit_code': int}, fail={'lines': list, 'exit_code': int})
    def up(self, message: str = 'chore: sc'):
        """
        Commit aller Aenderungen an versionierten Dateien (git commit -a) und push.
        Neue (untracked) Dateien werden nicht hinzugefuegt. Ohne Aenderungen wird nur gepusht.

        :param message: Commit-Message, Standard 'chore: sc'
        """
        message = message or 'chore: sc'
        lines = []
        changes = subprocess.run(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=self.path,
                                 capture_output=True, text=True)
        if changes.returncode != 0:
            return self.fail('git status failed', lines=(changes.stdout + changes.stderr).splitlines(),
                             exit_code=changes.returncode)
        if changes.stdout.strip() != '':
            line_ending_only = self.line_ending_only_changes()
            if len(line_ending_only) > 0:
                return self.fail(f'{len(line_ending_only)} files differ only in line endings (CRLF/LF), nothing committed. '
                                 f'Probably a checkout from another git (e.g. Windows vs. WSL).',
                                 lines=line_ending_only)
            commit = subprocess.run(['git', 'commit', '-am', message], cwd=self.path, capture_output=True, text=True)
            lines += (commit.stdout + commit.stderr).splitlines()
            if commit.returncode != 0:
                return self.fail('git commit failed', lines=lines, exit_code=commit.returncode)
        else:
            lines.append('Nothing to commit')
        try:
            push = subprocess.run(['git', 'push'], cwd=self.path, capture_output=True, text=True, timeout=120)
        except subprocess.TimeoutExpired:
            return self.fail('git push timeout', lines=lines)
        lines += (push.stdout + push.stderr).splitlines()
        if push.returncode != 0:
            return self.fail('git push failed', lines=lines, exit_code=push.returncode)
        return self.success('up', lines=lines, exit_code=0)

    def line_ending_only_changes(self) -> List[str]:
        """
        Versionierte Dateien, die sich gegenueber HEAD nur in den Zeilenenden unterscheiden.
        Entsteht z.B., wenn ein unter Windows (core.autocrlf=true) ausgechecktes Repo mit dem git der WSL benutzt wird.
        """
        def git_lines(*args) -> List[str]:
            res = subprocess.run(['git', *args], cwd=self.path, capture_output=True, text=True)
            return res.stdout.splitlines() if res.returncode == 0 else []
        changed = git_lines('diff', 'HEAD', '--name-only')
        # --numstat laesst mit --ignore-cr-at-eol Dateien weg, die sich nur in den Zeilenenden unterscheiden
        real = {line.split('\t', 2)[-1] for line in git_lines('diff', 'HEAD', '--numstat', '--ignore-cr-at-eol')}
        return [f for f in changed if f not in real]

    def remotes(self) -> List[dict]:
        """
        :return: [{'name': 'origin', 'url': 'git@...'}, ...]
        """
        return [{'name': remote.name, 'url': url} for remote in self.repo.remotes for url in remote.urls]

    WEB_HOSTS = {'github.com': 'GitHub', 'gitlab.com': 'GitLab', 'codeberg.org': 'Codeberg',
                 'bitbucket.org': 'Bitbucket', 'git.sr.ht': 'SourceHut'}

    # scp-artige Adresse wie git@github.com:user/repo.git, der Host muss einen Punkt enthalten (nicht C:/pfad)
    SCP_URL_RE = re.compile(r'^(?:[^@/\s]+@)?([\w-]+(?:\.[\w-]+)+):(?!//)(.+)$')

    @classmethod
    def remote_web_url(cls, url: str) -> Optional[str]:
        """
        Web-Adresse zu einer Remote-URL, z.B. git@github.com:user/repo.git -> https://github.com/user/repo

        :return: None bei lokalen Pfaden oder unbekanntem Format
        """
        url = url.strip()
        m = cls.SCP_URL_RE.match(url)
        if m is not None:
            host, path = m.group(1), m.group(2)
        else:
            parts = urlsplit(url)
            if parts.scheme not in ('https', 'http', 'ssh', 'git', 'git+ssh') or not parts.hostname:
                return None
            host, path = parts.hostname, parts.path
            if parts.scheme in ('https', 'http') and parts.port is not None:
                host += f':{parts.port}'  # ssh-Ports gehoeren nicht zur Web-Adresse
        path = path.strip('/')
        if path.endswith('.git'):
            path = path[:-4]
        if path == '':
            return None
        scheme = 'http' if url.startswith('http://') else 'https'
        return f'{scheme}://{host}/{path}'

    def web_links(self) -> List[dict]:
        """
        :return: [{'name': 'origin', 'title': 'GitLab', 'url': 'https://gitlab.com/...'}, ...], ohne doppelte Adressen
        """
        res = []
        for remote in self.remotes():
            url = self.remote_web_url(remote['url'])
            if url is not None and url not in [itm['url'] for itm in res]:
                host = urlsplit(url).hostname
                res.append({'name': remote['name'], 'title': self.WEB_HOSTS.get(host, host), 'url': url})
        return res

    def part_status(self, p: base.Page, params={}):
        s = self.status()
        p.prop("Branch", s['branch'] if s['branch'] is not None else "(detached)")
        p.prop("Commit", f"{(s['commit'] or '-')[:8]} {s['message'] or ''}")
        if s['tracking'] is not None:
            p.prop("Tracking", f"{s['tracking']} (ahead {s['ahead']}, behind {s['behind']})")
        p.prop("Dirty", s['dirty'])
        for title in ['staged', 'modified', 'untracked']:
            if len(s[title]) > 0:
                p.h3(title.capitalize())
                p.ul(list(map(self.link_file, s[title])))

    def blame(self, file, rev='HEAD'):
        # [ [git.Commit], [line1, line2], ... ]
        return u.List(list(map(lambda itm: BlameEntry(itm, file), self.repo.blame(rev, file))))

    def blame_all(self, source, doc, rev='HEAD'):
        blames = u.List()
        for f in self.glob(use_exclude_filter=True):
            if is_file_cat(f, source, doc):
                self.info("Blaming:" + f)
                try:
                    blames.add_all(self.blame(self.to_rel_path(f), rev))
                except Exception as e:
                    self.info('Error: ' + str(e))
        return blames

    def linestat(self, rev='HEAD') -> dict:
        blames = self.blame_all(source=True, doc=False, rev=rev)
        self.info("Blames: " + str(len(blames)))
        data = blames.group_by('author')
        data = dict(map(lambda kv: (kv[0], kv[1].sum('__len__')), data.items()))
        return data

    def part_index(self, p: base.Page, params={}):
        super().part_index(p, params)
        with p.section(h="Git"):
            p.ul([
                w.a("Status", self.link('status')),
                w.a("Blame Lines", self.link('linestat')),
                w.a("By Author", self.link('byauthor')),
                w.a("Authors", self.link('authors')),
                w.a("Remote", self.link('remote'))
            ])
        self.show_gitlab(p)
        self.part_deployments(p, params)

    def gitlab(self):
        """ GitlabProjectApi, wenn LOCAL.md gitlab_*-Parameter enthaelt, sonst None """
        from nxrunner.projects.gitlab import GitlabProjectApi
        if not GitlabProjectApi.is_configured(self.path):
            return None
        return GitlabProjectApi(self.path, [remote['url'] for remote in self.remotes()])

    def show_gitlab(self, p: base.Page):
        api = self.gitlab()
        if api is None:
            return
        with p.section(h="Gitlab"):
            api.part_summary(p)
            p.ul([w.a("Pipelines, MRs, Issues", self.link(self.part_gitlab), hint="GitLab-Details per API laden")])

    def part_gitlab(self, p: base.Page, params={}):
        api = self.gitlab()
        if api is None:
            p.div("Keine gitlab_*-Parameter in LOCAL.md")
            return
        api.part_gitlab(p, params)

    def project_names(self) -> set:
        """ Namen, unter denen das Repo als project_name eines Deployments vorkommen kann """
        return {n for n in (self.type, os.path.basename(str(self.path).rstrip('/\\'))) if n}

    def deployment_runners(self) -> list:
        """ Deployments-Runner im Dispatcher, per Spec registrierte werden dafuer erzeugt """
        from nxrunner.projects.deployments import Deployments
        disp = self.getParentClass(r.LazyDispatcher)
        res = []
        for name, entry in dict(getattr(disp, 'entries', None) or {}).items():
            if entry.loaded and isinstance(entry.instance, Deployments):
                res.append(entry.instance)
            elif not entry.loaded and str(entry.spec or '').split('(')[0].endswith(':Deployments'):
                runner = disp.get_runner(name)
                if isinstance(runner, Deployments):
                    res.append(runner)
        return res

    def part_deployments(self, p: base.Page, params={}):
        """ Section Deployments, wenn ein Deployments-Runner dieses Projekt enthaelt """
        for runner in self.deployment_runners():
            try:
                runner.part_project_deployments(p, self.project_names(), params)
            except Exception as e:
                self.error(f"Deployments von {runner.type}: {e}")

    def part_remote(self, p: base.Page, params={}):
        for r in self.repo.remotes:
            p.div("" + r.name + " " + r.url)

    def part_authors(self, p: base.Page, params={}):
        commits = list(self.repo.iter_commits())
        authors = list(set(map(lambda c: c.author.email, commits)))
        p.ul(list(map(lambda a: a + ": " + w.a("Blame", self.link('byoneauthor', 'author=' + a)), authors)))

    def part_byauthor(self, p: base.Page, params={}):
        blames = self.blame_all(source=True, doc=True, rev=params.get('rev', 'HEAD'))
        authors = blames.unique('author')
        for author in authors:
            p.h2("Author: " + str(author))
            f = None
            for blame in blames:
                if blame.author() == author:
                    if blame.file != f:
                        f = blame.file
                        p.h3(f)
                    p.pre(blame.text())

    def part_byoneauthor(self, p: base.Page, params={}):
        blames = self.blame_all(source=True, doc=True, rev=params.get('rev', 'HEAD'))
        authors = blames.unique('author')
        author = params.get('author', '')
        p.h2("Author: " + str(author))
        f = None
        for blame in blames:
            if blame.author() == author:
                if blame.file != f:
                    f = blame.file
                    p.h3(f)
                p.pre(blame.text())

    def part_linestat(self, p: base.Page, params={}):
        # TODO chartjs
        data = self.linestat(params.get('rev', 'HEAD'))
        p.barchart(data)
        for k, v in data.items():
            p.div(f'{k}: {v}')


class PythonRepo(GitRepo):
    """
    Git-Repository eines Python-Projekts, das pyproject.toml einliest, pip install ausfuehrt und den Modul-Abhaengigkeitsgraphen mit pydeps anzeigt.
    """

    EXCLUDE_FILTER = [*GitRepo.EXCLUDE_FILTER, 'build', 'dist', '*.egg-info', '__pycache__', '.venv', 'venv',
                      '.tox', '.pytest_cache', '.mypy_cache', '.eggs']

    def __init__(self, path='.'):
        super().__init__(path)
        self.define_sig(dev.PInt('pip_install', 1)).set_description('Führt pip install . im Repo-Verzeichnis aus').r(success=bool, message=str, text=str, exit_code=int)
        if self.exists('pyproject.toml'):
            self.read_toml()
        self.define_vars('version')

    def get_version(self):
        """ Paketversion aus pyproject.toml ([project] version), None wenn nicht vorhanden """
        project = self.props.get('project', {})
        return project.get('version') if isinstance(project, dict) else None

    def read_toml(self):
        try:
            import tomllib as tl
        except:
            import tomli as tl  # workarounf for python <= 3.10
        with open(os.path.join(self.path, "pyproject.toml"), "rb") as f:
            self.props = {**self.props, **tl.load(f)}

    def execute(self, data):
        if 'pip_install' in data:
            return self.pip_install_dir()
        return super().execute(data)

    def pip_install_dir(self):
        p = r.ProcessExecutor('pip install .', cwd=self.path)
        p.waitForEnd()
        return self.success(text=p.text(), exit_code=p.exit_code)

    def part_index(self, p: base.Page, params={}):
        super().part_index(p, params)
        with p.section(h="Python"):
            p(self.action_btn(dict(title="pip install", type=self.type, pip_install=1)))
            p.ul([w.a("pydeps", self.link(self.part_pydeps))])
        self.part_nxdoc(p)

    def package_candidates(self) -> list:
        """ moegliche Importnamen: [project] name aus pyproject.toml (- zu _), Runner-Typ, Verzeichnisname """
        project = self.props.get('project', {})
        names = [project.get('name') if isinstance(project, dict) else None, self.type,
                 os.path.basename(str(self.path).rstrip('/\\'))]
        res = []
        for n in names:
            n = (n or '').replace('-', '_')
            if n and n not in res:
                res.append(n)
        return res

    def package_name(self) -> Optional[str]:
        """ erster importierbarer Kandidat aus package_candidates, None wenn keiner installiert ist """
        import importlib.util
        for n in self.package_candidates():
            try:
                if importlib.util.find_spec(n) is not None:
                    return n
            except (ImportError, ValueError):
                pass
        return None

    @r.op("Eigenen Flask-Server fuer nxdoc starten (wenn nxdoc nicht in der Web-App eingehaengt ist)",
          result={'success': 'bool', 'message': 'str'})
    def nxdoc_server(self, host: str = '127.0.0.1', port: int = 7012):
        """ eigenen nxdoc-Server starten, wenn nxdoc nicht in der Web-App eingehaengt ist """
        from nxrunner.projects import documentation
        if documentation.start_server(host, port):
            return self.success('nxdoc: ' + documentation.base_url + 'documentation/')
        return self.fail('nxdoc-Server nicht gestartet: ' + documentation.server_error)

    def part_nxdoc(self, p: base.Page):
        """ Abschnitt Doku mit Link auf die nxdoc-Seite des Pakets, nur wenn nxdoc installiert ist """
        from nxrunner.projects import documentation
        pkg = self.package_name()
        if pkg is None or not documentation.installed():
            return
        documentation.add_package(pkg, self.path)
        with p.section(h="Doku"):
            url = documentation.url(pkg)
            if url is None:
                p.div("nxdoc ist nicht in die Web-App eingehängt.")
                p(self.action_btn(dict(title="nxdoc-Server starten", type=self.type, op='nxdoc_server'),
                                  hint="Startet einen eigenen Flask-Server für nxdoc (Port 7012)"))
            else:
                p.ul([w.a("nxdoc: " + html.escape(pkg), url)])

    def part_pydeps(self, p: base.Page, params={}):
        m = self.type
        # [-xx MODULE [MODULE ...]] [--only MODULE_PATH [MODULE_PATH ...]]
        cmd = f'pydeps {m} -o dependencies.svg --no-show --cluster --reverse ' + self.props.get('nx', {}).get('pydeps', '')
        r.ProcessExecutor(cmd, cwd=self.path).waitForEnd()
        time.sleep(0.1)
        if self.exists('dependencies.svg'):
            p(u.file_get_text(os.path.join(self.path, 'dependencies.svg')))


def main():
    args = u.Args()
    op = args.shift()
    if op == 'ci':
        sub = args.shift()
        ci = GitlabCi()
        if sub == '':
            ci.execute_ci(verbose=True)
        else:
            ci.execute_job(sub, verbose=True)


if __name__ == '__main__':
    main()
