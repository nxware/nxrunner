"""
Registry der installierten Runner (Entry-Points der Gruppe nweb_runner) fuer die UI.

Die Angaben (TAGS, MODULES, Docstring, Konstruktor-Parameter) werden statisch aus dem Quelltext gelesen,
ohne die Module zu importieren: schnell, ohne Nebenwirkungen und auch bei fehlenden Abhaengigkeiten.
Dynamisch gesetzte oder von Basisklassen geerbte TAGS werden dabei nicht erkannt.
"""
import ast
import importlib.metadata as md
import os
import sys

GROUP = 'nweb_runner'

# Projekte (erster Teil des Modulnamens eines Runners), die nicht oder nicht passend auf PyPI liegen,
# als pip-Anforderung (z.B. "name @ git+https://..."). Nicht aufgefuehrte Projekte werden von PyPI installiert.
PROJECTS = {
    'nxml': 'nxml @ git+https://github.com/b-oern/nxml.git'
}


def project(spec):
    """ 'nxml.image:ImageSimilarity' -> 'nxml' """
    return spec.split(':')[0].split('.')[0].strip()


def pip_requirement(project_name):
    """ pip-Anforderung fuer ein Projekt: aus PROJECTS, sonst der Name (PyPI) """
    return PROJECTS.get(project_name, project_name)

_module_files = {}


def module_file(module, dist=None):
    """
      Datei eines Moduls suchen, ohne es (oder Elternpakete) zu importieren: zuerst in der Distribution des
      Entry-Points, sonst ueber sys.path. Ergebnisse werden zwischengespeichert (Dateizugriffe auf
      Windows-Laufwerken in der WSL sind langsam).
    """
    if module in _module_files:
        return _module_files[module]
    parts = module.split('.')
    relative = ['/'.join(parts) + '.py', '/'.join(parts) + '/__init__.py']
    found = None
    if dist is not None:
        for rel in relative:
            try:
                candidate = str(dist.locate_file(rel))
            except Exception:
                continue
            if os.path.isfile(candidate):
                found = candidate
                break
    if found is None:
        for base in sys.path:
            base = base or os.getcwd()
            for rel in relative:
                candidate = os.path.join(base, *rel.split('/'))
                if os.path.isfile(candidate):
                    found = candidate
                    break
            if found:
                break
    _module_files[module] = found
    return found


def parse_file(path, cache):
    if path not in cache:
        with open(path, encoding='utf-8') as f:
            cache[path] = ast.parse(f.read(), filename=path)
    return cache[path]


def literal(node):
    """ Wert eines Ausdrucks, wenn er ein Literal ist, sonst der Quelltext """
    try:
        return ast.literal_eval(node)
    except Exception:
        try:
            return ast.unparse(node)
        except Exception:
            return None


def resolve_relative(module, level, name):
    """ from .x import y in module (Datei-Modul) -> absoluter Modulname """
    if level == 0:
        return name
    path = module_file(module) or ''
    # Paket des Moduls: bei __init__.py das Modul selbst, sonst das uebergeordnete
    package = module if path.endswith('__init__.py') else module.rpartition('.')[0]
    for _ in range(level - 1):
        package = package.rpartition('.')[0]
    return package + ('.' + name if name else '')


def find_class(module, name, cache, depth=0, dist=None):
    """ ClassDef zu module:name, folgt dabei Re-Exporten (from x import name). :return: (ClassDef, module_doc) """
    path = module_file(module, dist)
    if path is None or depth > 5:
        return None, None
    tree = parse_file(path, cache)
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node, ast.get_docstring(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if (alias.asname or alias.name) == name:
                    source = resolve_relative(module, node.level, node.module or '')
                    return find_class(source, alias.name, cache, depth + 1, dist)
    return None, None


def class_info(cls: ast.ClassDef) -> dict:
    """ TAGS, MODULES, Docstring und Konstruktor-Parameter einer Klasse """
    info = {'tags': [], 'modules': [], 'doc': ast.get_docstring(cls) or '', 'params': [], 'bases': []}
    info['bases'] = [ast.unparse(b) for b in cls.bases]
    for node in cls.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            key = node.targets[0].id
            if key in ('TAGS', 'MODULES'):
                value = literal(node.value)
                info[key.lower()] = [str(v) for v in value] if isinstance(value, (list, tuple, set)) else []
        elif isinstance(node, ast.FunctionDef) and node.name == '__init__':
            args = node.args
            positional = args.posonlyargs + args.args
            defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
            for arg, default in zip(positional, defaults):
                if arg.arg == 'self':
                    continue
                info['params'].append({'name': arg.arg, 'default': literal(default) if default is not None else None,
                                       'required': default is None})
            for arg, default in zip(args.kwonlyargs, args.kw_defaults):
                info['params'].append({'name': arg.arg, 'default': literal(default) if default is not None else None,
                                       'required': default is None})
    return info


def normalize(name):
    """ pip-Namen vergleichbar machen (PEP 503): Gross-/Kleinschreibung, - _ . egal """
    return '-'.join(name.replace('_', '-').replace('.', '-').lower().split('-'))


def installed_distributions() -> set:
    return {normalize(d.metadata['Name']) for d in md.distributions() if d.metadata['Name']}


def installed(distribution, available=None):
    """ ist ein pip-Paket (MODULES-Eintrag) installiert? """
    name = distribution.split('[')[0].split('=')[0].split('<')[0].split('>')[0].strip()
    if available is None:
        available = installed_distributions()
    return normalize(name) in available


def entries() -> list:
    """
      [{name, value, package, version, cls, tags, modules, missing, doc, summary, params, error}]
      name ist der Name des Entry-Points (Vorschlag fuer den type beim Laden)
    """
    cache = {}
    res = []
    seen = set()
    available = installed_distributions()
    dist_meta = {}  # Metadaten je Distribution nur einmal lesen
    for ep in sorted(md.entry_points().select(group=GROUP), key=lambda e: e.name):
        if ep.name in seen:
            continue
        seen.add(ep.name)
        dist = getattr(ep, 'dist', None)
        if dist is not None and id(dist) not in dist_meta:
            dist_meta[id(dist)] = (dist.metadata['Name'] or '', dist.version or '')
        package, version = dist_meta.get(id(dist), ('', ''))
        item = {'name': ep.name, 'value': ep.value, 'package': package, 'version': version, 'tags': [], 'modules': [],
                'missing': [], 'doc': '', 'summary': '', 'params': [], 'error': None}
        module, _, attr = ep.value.partition(':')
        item['cls'] = attr.split('(')[0].strip()
        try:
            cls, module_doc = find_class(module.strip(), item['cls'], cache, dist=dist)
            if cls is None:
                item['error'] = 'Klasse im Quelltext nicht gefunden'
            else:
                item.update(class_info(cls))
                if not item['doc'] and module_doc:
                    item['doc'] = module_doc
        except Exception as e:
            item['error'] = f'{type(e).__name__}: {e}'
        item['missing'] = [m for m in item['modules'] if not installed(m, available)]
        item['summary'] = next((line.strip() for line in item['doc'].splitlines() if line.strip()), '')
        res.append(item)
    return res


def entry(name):
    ep = md.entry_points().select(group=GROUP, name=name)
    for e in ep:
        return e
    return None


def parse_value(value):
    """ Formularwert -> Python-Wert (Zahl, bool, None, Liste, ...), sonst Text """
    if not isinstance(value, str):
        return value
    try:
        return ast.literal_eval(value)
    except Exception:
        lower = value.strip().lower()
        if lower in ('true', 'false'):
            return lower == 'true'
        return value
