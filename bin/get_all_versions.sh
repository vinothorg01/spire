#!/bin/sh
SPIRE_VERSION=$(python -c "_dct = {}; exec(open('./spire/version.py').read(), _dct); print(_dct['__version__'])")
echo 'Spire: '$SPIRE_VERSION
DATASCI_COMMON_VERSION=$(python -c "_dct = {}; exec(open('./include/datasci-common/python/datasci/version.py').read(), _dct); print(_dct['__version__'])")
echo 'Datasci-common: '$DATASCI_COMMON_VERSION
KALOS_VERSION=$(python -c "_dct = {}; exec(open('./include/kalos/kalos/version.py').read(), _dct); print(_dct['__version__'])")
echo 'Kalos: '$KALOS_VERSION
