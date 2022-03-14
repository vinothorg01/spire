#!/bin/bash
#######################################
#
# Copies the Falcon wheel to DBFS
#
#######################################
upload_to_dbfs() {
  local DEPLOY_ENVIRON="${1}"
  local BUILDER="$(whoami)"
  FALCON_DBFS=None
  for entry in ./dist/*
  do
    FALCON_WHL="$entry"
    FALCON_WHL=${FALCON_WHL#*./dist/}
  done
  # echo ${FALCON_WHL}
  if [ $DEPLOY_ENVIRON = "dev" ]; then
    FALCON_DBFS=dbfs:/cn-falcon-dev/development/${BUILDER}/${FALCON_WHL}
#    dbfs cp dist/*.whl "$FALCON_DBFS" --overwrite
  fi
  echo ${FALCON_DBFS}
}
#######################################
#
# Main
#
#######################################
DEPLOY_ENVIRON=$1
echo "${DEPLOY_ENVIRON}"
rm -rf dist/ build/ falcon.egg-info
python setup.py bdist_wheel
# upload_to_dbfs dev
echo "success!"
exit 0
