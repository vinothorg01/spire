VERSION=$(sed -n "s/.*\__version__ = \"\\([^']*\)\"\.*/\1/p" falcon/__init__.py)
echo ${VERSION}
