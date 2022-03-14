from setuptools import find_packages, setup
from falcon import __version__ as version

with open("README.md", "r") as f:
    long_description = f.read()

with open("LICENSE", "r") as f:
    license_str = f.read()

with open("requirements.txt", "r") as f:
    requirements = f.readlines()

packages = find_packages()

setup(
    name="falcon",
    packages=packages,
    version=version,
    description="Predicting Content Virality to drive Social Traffic",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Anuj Katiyal, Max Cantor",
    license=license_str,
    install_requires=requirements,
    python_requires=">=3.7",
    url="https://github.com/CondeNast/datasci-virality",
    package_data={
        "": ["settings.yaml", "brand_exclusions.yaml"]
    },  # https://setuptools.readthedocs.io/en/latest/setuptools.html#including-data-files
)
