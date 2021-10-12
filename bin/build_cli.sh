#!/bin/bash

set -ETeu -o pipefail

#######################################
#
# Builds the Spire cli module
#
#######################################
function build_cli_package() {
  python -m pip install -e ./cli
}


#######################################
#
# Main
#
#######################################
build_cli_package
echo "cli built!"