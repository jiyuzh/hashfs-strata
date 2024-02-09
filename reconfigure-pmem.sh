#!/usr/bin/env bash

set -euo pipefail

sudo ndctl destroy-namespace -f all
sudo ndctl create-namespace -m devdax -f -e namespace0.0 -s 400G
sudo ndctl create-namespace -m devdax -f -e namespace1.0 -s 128G
sudo ndctl create-namespace -m devdax -f -e namespace1.1 -s 128G
