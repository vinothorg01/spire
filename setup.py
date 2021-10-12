from setuptools import setup, find_packages
import subprocess


PACKAGE_EXCLUDE_DIRS = ["docs", "examples", "notebooks", "tests"]

with open("./requirements.txt", "r") as f:
    requirements = f.read().strip().split("\n")

proc = subprocess.Popen("./bin/get_version.sh", stdout=subprocess.PIPE)
__version__ = proc.stdout.read().decode('utf-8').strip('\n')

setup(
    name="spire",
    version=__version__,
    description="The codebase behind Condé Nast's smart data platform Spire",
    author="Condé Nast",
    packages=find_packages(exclude=PACKAGE_EXCLUDE_DIRS),
    install_requires=requirements,
)
