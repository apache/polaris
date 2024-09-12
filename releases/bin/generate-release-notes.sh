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
bin_dir="$(dirname "$0")"
command_name="$(basename "$0")"
. "${bin_dir}/_lib.sh"

new_major_version=""
new_minor_version=""
patch_version=""
prev_version_full=""

function usage {
  cat <<!
Usage: $0 --major <major-version> --minor <minor-version> --patch <patch-version> --previous [<prev-version-full>]

  major-version           Major version being released
  minor-version           Minor version being released
  patch-version           Patch version being released
  previous-version-full   Optional: a different major.minor.patch version of the PREVIOUS release

If 'previous-version-full' is not given, it will be inferred from the new major/minor/patch version.
For a patch-version grater than 0, it will be new-patch-version minus 1.
For a minor-version grater than 0, it will be new-minor-version minus 1 and patch-version 0.
For a major-version grater than 0, it will be new-major-version minus 1 and minor-version 0 and patch-version 0.
!
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --major)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Missing argument for --major, aborting" > /dev/stderr
        exit 1
      fi
      new_major_version="$1"
      shift
      ;;
    --minor)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Missing argument for --minor, aborting" > /dev/stderr
        exit 1
      fi
      new_minor_version="$1"
      shift
      ;;
    --patch)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Missing argument for --patch, aborting" > /dev/stderr
        exit 1
      fi
      patch_version="$1"
      shift
      ;;
    --previous)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Missing argument for --previous, aborting" > /dev/stderr
        exit 1
      fi
      prev_version_full="$1"
      shift
      ;;
    *)
      usage > /dev/stderr
      exit 1
      ;;
  esac
done

if [[ -z ${new_major_version} ]]; then
  echo "Mandatory --major option missing, aborting" > /dev/stderr
  exit 1
fi
if [[ -z ${new_minor_version} ]]; then
  echo "Mandatory --minor option missing, aborting" > /dev/stderr
  exit 1
fi
if [[ -z ${patch_version} ]]; then
  echo "Mandatory --patch option missing, aborting" > /dev/stderr
  exit 1
fi

if [[ -z ${prev_version_full} ]]; then
  if [[ ${patch_version} -gt 0 ]] ; then
    prev_version_full="${new_major_version}.${new_minor_version}.$((${patch_version} - 1))"
    since_label="Changes since Apache Polaris version ${new_major_version}.${new_minor_version}.$((${patch_version} - 1))"
  elif [[ ${new_minor_version} -gt 0 ]] ; then
    prev_version_full="${new_major_version}.$((${new_minor_version} - 1)).0"
    since_label="Changes since Apache Polaris version ${new_major_version}.$((${new_minor_version} - 1))"
  elif [[ ${new_minor_version} -gt 0 ]] ; then
    prev_version_full="$((${new_major_version} - 1)).0.0"
    since_label="Changes since Apache Polaris major version $((${new_major_version} - 1))"
  else
    prev_version_full="0.0.0"
  fi
fi

prev_tag="${tag_prefix}${prev_version_full}"

version_full="${new_major_version}.${new_minor_version}.${patch_version}"

total_contributor_count="$(curl https://api.github.com/repos/apache/polaris/contributors?per_page=5000 2>/dev/null | jq -r .[].login | grep --invert-match --extended-regexp "^(dependabot|renovate|sfc-).*" | sort | wc -l )"

cat <<!
---
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
linkTitle: Release Notes
title: Release Notes for Apache Polaris version ${version_full}
type: docs
weight: 1
---

## Contributors

The Apache Polaris project thanks all ${total_contributor_count} contributors who made Apache Polaris possible!!
In particular for this release, thank you to $(git rev-list --reverse HEAD ^"${prev_tag}" --format="%an" | grep --invert-match --extended-regexp "commit |dependabot\[bot\]|renovate\[bot\]" | sort | uniq | paste -s -d, | sed 's/,/, /g').

$(tail -n+19 "${worktree_dir}/NEXT_RELEASE_NOTES.md")

## New commits in version ${version_full} since ${prev_version_full}

The following is the verbose list of all commits in chronological order (reverse commit log).
!

if [[ -n "${since_label}" ]]; then
  echo "${since_label}"
  echo ""
fi
# Identify commits that are reachable from HEAD but not from the previous version Git tag.
# In other words: all commits between the previous version and HEAD.
sep_start="----LOG_SEPARATOR_START_THAT_MUST_NOT_APPEAR_IN_COMMIT_MESSAGES----"
sep_end="----LOG_SEPARATOR_END_THAT_MUST_NOT_APPEAR_IN_COMMIT_MESSAGES----"
in_log=0
commit_hash=""
commit_hash_abbrev=""
commit_author=""
gh_pr_link_replacement="s/#([0-9]+)/[#\1](https:\/\/github.com\/apache\/polaris\/pull\/\1)/g"
git rev-list --reverse HEAD ^"${prev_tag}" --format="${sep_start}%+H,%h,%an%+s%+b%n${sep_end}" | while read ln ; do
  case $in_log in
    0)
      if [[ "${ln}" == "${sep_start}" ]] ; then
        in_log=1
      fi
      ;;
    1)
      # hash,hash_abbrev,author
      in_log=2
      commit_hash="$(echo "${ln}" | cut -d, -f1)"
      commit_hash_abbrev="$(echo "${ln}" | cut -d, -f2)"
      commit_author="$(echo "${ln}" | cut -d, -f3)"
      ;;
    2)
      ## subject
      in_log=3
      subject="$(echo "${ln}" | sed --regexp-extended "${gh_pr_link_replacement}")"
      echo "* **${subject}** ([commit ${commit_hash_abbrev}](https://github.com/apache/polaris/commit/${commit_hash}) by *${commit_author}*)"
      echo ""
      ;;
    3)
      if [[ "${ln}" == "${sep_end}" ]] ; then
        # end of commit
        in_log=0
      else
        # Indent and replace GH links
        echo "  ${ln}" | sed --regexp-extended "${gh_pr_link_replacement}"
      fi
      ;;
  esac
done
