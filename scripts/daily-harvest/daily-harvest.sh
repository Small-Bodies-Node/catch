#!/bin/bash -eu
#
# example crontab entry, check at minute 13 past every 7th hour
# 13 */7 * * * cd /path/to/daily-harvest && /bin/bash daily-harvest.sh

LOGROTATE=/usr/sbin/logrotate

source .venv/bin/activate

# Rotate logs and CSS file list.
$LOGROTATE logrotate.config -s logrotate.state

if [[ ! -e catch.config ]]; then
    echo "catch.config missing"
    exit 1
fi

# If the CSS file list was not rotated, then use it.
OPTS=
if [[ -e css-file-list.txt ]]; then
    OPTS="-f css-file-list.txt"
fi
python3 .venv/src/catch/scripts/add-css.py $OPTS
