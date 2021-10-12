#!/bin/bash

set -e

/etc/init.d/dbus start 
service datadog-agent restart

exec "$@"
