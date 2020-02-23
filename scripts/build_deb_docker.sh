#!/bin/bash

VERSION="$1"
RELEASE="$2"

. ~/.cargo/env

cargo build --release

printf "Sync files between computers and cloud storage services\n" > description-pak
echo checkinstall --pkgversion ${VERSION} --pkgrelease ${RELEASE} -y
checkinstall --pkgversion ${VERSION} --pkgrelease ${RELEASE} -y
