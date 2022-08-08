#! /bin/bash
set -euo pipefail
shopt -s expand_aliases
 
source ci/bootstrap/src/bootstrap.sh
export PATH="$PATH:$PWD/ci/bootstrap/bin"
export no_proxy="$no_proxy,elasticsearch,postgres"
export NO_PROXY="$NO_PROXY,elasticsearch,postgres"
