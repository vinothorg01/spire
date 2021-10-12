#!/bin/sh
SPIRE_SAFE_VERSION=$(python -c "_dct = {}; exec(open('./spire/version.py').read(), _dct); from pkg_resources import safe_version;
print(safe_version(_dct['__version__']))")
echo $SPIRE_SAFE_VERSION
