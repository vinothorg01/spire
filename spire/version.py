from pkg_resources import safe_version
import subprocess

# follows semver
__version__ = "4.2.0-dev"

# pep-440 compliant version, use for things like wheel name
# see https://www.python.org/dev/peps/pep-0440/ for now to name your version
__safe_version__ = safe_version(__version__)


def get_all_module_versions():
    proc = subprocess.Popen("./bin/get_all_versions.sh", stdout=subprocess.PIPE)
    __versions__ = proc.stdout.read().decode("utf-8").strip("\n")
    return __versions__
