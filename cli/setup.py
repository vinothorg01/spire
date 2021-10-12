"""Spire CLI setup information"""

from setuptools import setup, find_namespace_packages

setup(
    name="spirecli",
    version="2.0",
    packages=find_namespace_packages(include=["cli.*", "../spire.*"]),
    install_requires=["Click", "click-shell"],
    entry_points = {
        'console_scripts': [
            'spire-version = spire.version:get_all_module_versions',
            'spire=__init__:cli'
    ],
    }
)
