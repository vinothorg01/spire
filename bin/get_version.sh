#!/bin/sh
SPIRE_VERSION=$(python -c "_dct = {}; exec(open('./spire/version.py').read(), _dct); print(_dct['__version__'])")
echo $SPIRE_VERSION
