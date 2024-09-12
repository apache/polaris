#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e

cd "$(dirname "$0")/.."

force=
while [[ $# -gt 0 ]]; do
  case $1 in
    --force)
      force=1
      shift
      ;;
    *)
      echo "Unexpected argument '$1'" > /dev/stderr
      exit 1
      ;;
  esac
done

if [[ ${force} ]] ; then
  rm -rf content/releases
  git worktree prune
else
  if [[ ! -d content/releases ]] ; then
    echo "Directory content/releases does not exists" > /dev/stderr
    exit 1
  fi

  cd content/releases
  if git diff-index --quiet HEAD -- ; then
    cd ../..
    rm -rf content/releases
    git worktree prune
  else
    echo "Directory content/releases contains uncommitted changes" > /dev/stderr
    exit 1
  fi
fi
