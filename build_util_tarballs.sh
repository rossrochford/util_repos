#!/bin/bash

DESTINATION_DIRS=$1
PARENT_WORKING_DIR=$(readlink --canonicalize ".")

build_project () {
  cd "/home/ross/code/util_repos" || exit
  cd "$1" || exit
  rm -f "dist/$2"
  source venv/bin/activate
  python setup.py sdist
}


build_project "mongodb_util/" "mongodb-util-0.0.1.tar.gz"
for currDest in "$@"; do
    rm -f "$PARENT_WORKING_DIR/$currDest/mongodb-util-0.0.1.tar.gz"
    cp "dist/mongodb-util-0.0.1.tar.gz" "$PARENT_WORKING_DIR/$currDest/mongodb-util-0.0.1.tar.gz"
done


build_project "redis_util/" "redis-util-0.0.1.tar.gz"
for currDest in "$@"; do
    rm -f "$PARENT_WORKING_DIR/$currDest/redis-util-0.0.1.tar.gz"
    cp "dist/redis-util-0.0.1.tar.gz" "$PARENT_WORKING_DIR/$currDest/redis-util-0.0.1.tar.gz"
done


build_project "trio_util/" "trio-util-0.0.1.tar.gz"
for currDest in "$@"; do
    rm -f "$PARENT_WORKING_DIR/$currDest/trio-util-0.0.1.tar.gz"
    cp "dist/trio-util-0.0.1.tar.gz" "$PARENT_WORKING_DIR/$currDest/trio-util-0.0.1.tar.gz"
done


build_project "util_shared/" "util-shared-0.0.1.tar.gz"
for currDest in "$@"; do
    rm -f "$PARENT_WORKING_DIR/$currDest/util-shared-0.0.1.tar.gz"
    cp "dist/util-shared-0.0.1.tar.gz" "$PARENT_WORKING_DIR/$currDest/util-shared-0.0.1.tar.gz"
done
