
from nwebclient import util as u
import importlib.metadata as md
import importlib
import sys
import subprocess


def exec(cmd):
    print(f"Exec: {cmd}")
    result = subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if result.stdout:
        print(result.stdout, end="")

    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return result.returncode

def deps_for_pkg(dist_name):
    res = []
    dist = md.distribution(dist_name)
    print(f"Runners for {dist_name}")
    for ep in dist.entry_points:
        if ep.group == 'nweb_runner':
            print(" " + ep.name + " = " + ep.value)
            try:
                cls = u.load_class(ep.value, False)
                ms = getattr(cls, 'MODULES', [])
                print("   Modules: " + str(ms) )
                res = [*res, *ms]
            except ModuleNotFoundError as e:
                print("Error importing " + e.name)
                res = [*res, e.name]
    return res


def deps():
    print("Deps")


    def list_installed_packages(self):
        """
          Default: pip, wheel, setuptools
        :return:
        """
        distributions = importlib.metadata.distributions()
        # installed_packages = []
        # for dist in distributions:
        #    args = (dist.metadata['Name'], dist.version)
        #    installed_packages.append(args)
        # installed_packages.sort()  # Sort the packages by name
        # for package_name, version in installed_packages:
        #    print(f"{package_name}=={version}")
        return distributions

    def list_runner_packages(self):
        res = []
        for pkg in self.list_installed_packages():
            for ep in pkg.entry_points:
                if ep.group == 'nweb_runner':
                    res.append(pkg)
                    break

    def get_distribution_name():
        module = __package__ or __name__.split('.')[0]
        mapping = md.packages_distributions()
        dists = mapping.get(module, [])
        return dists[0] if dists else None

    dist_name = get_distribution_name()

    if dist_name:
        dist = md.distribution(dist_name)
        for ep in dist.entry_points:
            print(ep.group, ep.name, ep.value)
    else:
        print("No Dist found")


def install(pkg):
    print(f"Install: pkg")
    try:
        importlib.import_module(pkg)
        install_deps(pkg)
    except ModuleNotFoundError:
        print("Install via pip")
        if exec(sys.executable + " -m pip install " + pkg) > 0:
            print("Error install.")
        exec(sys.executable + " -m nxrunner install_deps " + pkg)


def install_deps(pkg):
    deps = deps_for_pkg(pkg)
    print("Deps: " + str(deps))
    for dep in deps:
        exec(sys.executable + " -m pip install " + dep)
    print("Done.")


def help():
    print("Usage:")
    print("")
    print("  python -m nxrunner deps")
    print("  python -m nxrunner install pkg")


args = u.Args(read_local_only=True)
args.shift()
action = args.shift('run')
match action:
    case "deps":
        deps_for_pkg("nxrunner")
    case 'install':
        install(args.shift('nxrunner'))
    case 'install_deps':
        install_deps(args.shift('nxrunner'))
    case _:
        print("Action" + action)
        help()
