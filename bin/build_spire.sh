#!/bin/bash

set -ETeu -o pipefail

#######################################
#
# Builds the Spire Python module
#
#######################################
function build_spire_dist() {
  python setup.py bdist_wheel
}


#######################################
#
# Copies the Spire egg to DBFS
#
#######################################
function upload_to_dbfs() {
  local DEPLOY_ENVIRON="${1}"
  local BUILDER="$(whoami)"
  SPIRE_DBFS=None
  for entry in ./dist/*
  do
    SPIRE_WHL="$entry"
    SPIRE_WHL=${SPIRE_WHL#*./dist/}
  done
  echo ${SPIRE_WHL}
  if [ $DEPLOY_ENVIRON = "personal" ]; then
    SPIRE_DBFS=dbfs:/spire/development/${BUILDER}/dist/${SPIRE_WHL}
  fi
  if [ $DEPLOY_ENVIRON = "development" ]; then
    SPIRE_DBFS=dbfs:/spire/development/dist/${SPIRE_WHL}
  fi
  if [ $DEPLOY_ENVIRON = "staging" ]; then
    SPIRE_DBFS=dbfs:/spire/staging/dist/${SPIRE_WHL}
  fi
  if [ $DEPLOY_ENVIRON = "production" ]; then
    SPIRE_DBFS=dbfs:/spire/production/dist/${SPIRE_WHL}
  fi
  echo ${SPIRE_DBFS}
  dbfs rm ${SPIRE_DBFS}
  dbfs cp dist/*.whl ${SPIRE_DBFS}
  rm -rf dist/ build/
}


#######################################
#
# Main
#
#######################################
DEPLOY_ENVIRON=$1
echo "${DEPLOY_ENVIRON}"
build_spire_dist
upload_to_dbfs ${DEPLOY_ENVIRON}
echo "building cli"
sh ./bin/build_cli.sh
echo "success!"
exit 0
