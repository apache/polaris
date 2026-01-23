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
set -o pipefail

cd "$(dirname "$0")/.."

if [[ ! -d content/releases ]] ; then
  echo "Directory content/releases does not exists, run bin/checkout-releases.sh first."
  exit 1
fi

versions=()
while [[ $# -ge 1 ]]; do
  arg="$1"
  shift
  case "$arg" in
  -*)
    ;;
  *)
    versions+=("$arg")
  esac
done

for version in "${versions[@]}"; do
  echo ""
  echo "Marking all $version docs files to not be indexed..."
  while IFS= read -r file; do
    echo " .. processing file ${file}"
    readarray -t all_lines < "${file}"
    if [[ ${all_lines[0]} == "---" ]]; then
      echo "   .. has front matter"
      end_index=1
      while [[ ${all_lines[$end_index]} != "---" ]]; do
        end_index=$((end_index+1))
      done

      front_matter=("${all_lines[@]:1:$((end_index-1))}")
      updated=0
      if ! printf '%s\n' "${front_matter[@]}" | grep -qE '^robots: ' ; then
        front_matter+=('robots: noindex')
        updated=1
      fi
      if ! printf '%s\n' "${front_matter[@]}" | grep -qE '^exclude_search: ' ; then
        front_matter+=('exclude_search: true')
        updated=1
      fi
      if [[ $updated == 1 ]]; then
        echo "    .. updating (${#front_matter[@]} lines)"
        (
          echo "---"
          printf '%s\n' "${front_matter[@]}"
          printf '%s\n' "${all_lines[@]:$end_index:${#all_lines[@]}}"
        ) > "${file}.tmp"
        rm "${file}"
        mv "${file}.tmp" "${file}"
      fi
    else
      echo "   .. NO front matter in file"
    fi
  done < <(find "content/releases/$version" -name "*.md")
done
