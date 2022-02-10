#!/bin/bash -eu

source .venv/bin/activate

# Rotate logs and CSS file list.
logrotate logrotate.config -s logrotate.state

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
