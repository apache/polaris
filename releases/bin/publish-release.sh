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
bin_dir="$(dirname "$0")"
command_name="$(basename "$0")"
. "${bin_dir}/_releases_lib.sh"

echo "Version in version.txt is '$version_txt'"
echo "Current branch is '${current_branch}' on remote '${upstream_name}'"
echo ""

function usage {
  cat << EOF
${command_name}
    [--major MAJOR_VERSION] [--minor MINOR_VERSION]
    [--dry-run]
    [--help | -h]

  Promotes a release candidate to a final version.

  This command looks up the latest release candidate for the latest patch version of the major/minor version
  and promote it. Promotion will fail, if a promoted tag (no '-rcX' suffix) for the latest patch version
  already exists.

  Options:
    --major MAJOR_VERSION
        Major version number, must be specified when the command is called on the main branch.
    --minor MINOR_VERSION
        Minor version number, must be specified when the command is called on the main branch
        or on a major version branch.
    --dry-run
        Do not update Git. Gradle will publish to the local Maven repository, but still sign the artifacts.
    -h --help
        Print usage information.
EOF
}

dry_run=
new_major_version="${version_major}"
new_minor_version="${version_minor}"
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
    --dry-run)
      dry_run=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option/argument $1" > /dev/stderr
      usage > /dev/stderr
      exit 1
      ;;
  esac
done

new_tag_name=""
from_tag_name=""
case "${branch_type}" in
  "main")
    if [[ -z ${new_major_version} ]]; then
      echo "On the main branch, but specified no major version using the '--major' argument, aborting" > /dev/stderr
      exit 1
    fi
    if [[ -z ${new_minor_version} ]]; then
      echo "On the main branch, but specified no minor version using the '--minor' argument, aborting" > /dev/stderr
      exit 1
    fi
    ;;
  "major")
    if [[ ${version_major} -ne ${new_major_version} ]]; then
      echo "On the major version branch ${version_major}, but specified '--major ${new_major_version}', must be on a the matching version branch, aborting" > /dev/stderr
      exit 1
    fi
    if [[ -z ${new_minor_version} ]]; then
      echo "On the major version branch ${version_major}, but specified no minor version using the '--minor' argument, aborting" > /dev/stderr
      exit 1
    fi
    ;;
  "minor")
    if [[ ${version_major} -ne ${new_major_version} || ${version_minor} -ne ${new_minor_version} ]]; then
      echo "On the minor version branch ${version_major}, but specified '--major ${new_major_version}', must be on a the matching version branch, aborting" > /dev/stderr
      exit 1
    fi
    ;;
  *)
    echo "Unexpected branch type ${branch_type}" > /dev/stderr
    exit 1
esac

patch_version="$(get_max_patch_version ${new_major_version} ${new_minor_version})"
rc_iteration=
if [[ $patch_version -eq -1 ]]; then
  # that patch version is released
  echo "Version ${new_major_version}.${new_minor_version}.x has no drafted patch release, aborting" > /dev/stderr
  exit 1
else
  version_full_base="${new_major_version}.${new_minor_version}.${patch_version}"
  rc_iteration="$(get_max_rc_iteration "${version_full}")"
  if [[ $rc_iteration -eq -2 ]]; then
    # that patch version is released
    echo "Version ${version_full} is already released, aborting" > /dev/stderr
    exit 1
  elif [[ $rc_iteration -eq -1 ]]; then
    echo "Unexpected result -1 from get_max_rc_iteration function, aborting" > /dev/stderr
    exit 1
  fi
fi
version_incubating="$(get_podling_version_suffix)"
version_full="${version_full_base}${version_incubating}"
from_tag_name="${tag_prefix}${version_full_base}-rc${rc_iteration}"
new_tag_name="${tag_prefix}${version_full_base}"

echo ""
if [[ ${dry_run} ]]; then
  echo "Dry run, no changes will be made - except the versioned docs updates for local inspection"
else
  echo "Non-dry run, will update Git"
fi

cd "${worktree_dir}"
echo "Changed to directory $(pwd)"

echo ""
echo "From tag name is: ${from_tag_name}"
echo "New tag name is:  ${new_tag_name}"
echo ""
git log -n1 "${from_tag_name}"
echo ""

if [[ -z ${new_tag_name} || -z ${from_tag_name} ]]; then
  echo "Empty tag to create - internal error, aborting" > /dev/stderr
  exit 1
fi

exec_process 0 git checkout "${from_tag_name}"
cleanups+=("git checkout ${current_branch}")



start_group "Upload source tarball"
cd "${worktree_dir}"
rm -rf "${svn_dir_dev}"
mkdir -p "${svn_dir_dev}"
cd "${svn_dir_dev}"
echo "Changed to directory $(pwd)"

if [[ ${dry_run} == 1 || -z ${CI} ]]; then
  # If not in CI and/or dry-run mode is being used, use a local SVN repo for simulation purposes
  echo "Using local, temporary svn for 'dev' for dry-run mode - as a local replacement for ${svn_dist_dev_repo}"
  svn_local_dummy_dev="$(realpath ../svn-local-dummy-dev)"
  if [[ ! -d ${svn_local_dummy_dev} ]] ; then
    echo "Expecting existing local dummy svn for 'dev' for dry-run mode, created by dry-run draft-release.sh" > /dev/stderr
    exit 1
  fi
  exec_process 0 svn checkout "file://${svn_local_dummy_dev}" .
else
  exec_process 0 svn checkout "${svn_dist_dev_repo}" .
fi

cd "${worktree_dir}"
rm -rf "${svn_dir_release}"
mkdir -p "${svn_dir_release}"
cd "${svn_dir_release}"
echo "Changed to directory $(pwd)"

if [[ ${dry_run} == 1 || -z ${CI} ]]; then
  # If not in CI and/or dry-run mode is being used, use a local SVN repo for simulation purposes
  echo "Using local, temporary svn for 'release' for dry-run mode - as a local replacement for ${svn_dist_release_repo}"
  svn_local_dummy_release="$(realpath ../svn-local-dummy-release)"
  if [[ ! -d ${svn_local_dummy_release} ]]; then
    exec_process 0 svnadmin create "${svn_local_dummy_release}"
  fi
  exec_process 0 svn checkout "file://${svn_local_dummy_release}" .
else
  exec_process 0 svn checkout "${svn_dist_release_repo}" .
fi

if [[ -d "${version_full}" ]]; then
  # Delete previous patch versions
  find "${new_major_version}.${new_minor_version}.*" -type f -exec svn rm --force {} +
fi
mkdir -p "${version_full}"
# Copy the source tarball files from the release candidate
cp ${worktree_dir}/${svn_dir_dev}/${version_full}/RC${rc_iteration}/* "${version_full}"
exec_process 0 svn add --force "${version_full}"
exec_process 0 svn commit -m "Polaris ${version_full} release"

cd "${worktree_dir}/${svn_dir_dev}"
echo "Changed to directory $(pwd)"
exec_process 0 svn rm --force "${version_full}"
exec_process 0 svn commit -m "Remove RC${rc_iteration} for published Polaris ${version_full} release"

cd "${worktree_dir}"
echo "Changed to directory $(pwd)"
end_group



start_group "Releasing staging repository"
if [[ ${CI} ]]; then

  if [[ -z ${ORG_GRADLE_PROJECT_sonatypeUsername} || -z ${ORG_GRADLE_PROJECT_sonatypePassword} ]] ; then
    echo "One or more of the following required environment variables are missing:" > /dev/stderr
    [[ -z ${ORG_GRADLE_PROJECT_sonatypeUsername} ]] && echo "  ORG_GRADLE_PROJECT_sonatypeUsername" > /dev/stderr
    [[ -z ${ORG_GRADLE_PROJECT_sonatypePassword} ]] && echo "  ORG_GRADLE_PROJECT_sonatypePassword" > /dev/stderr
    exit 1
  fi

  stagingRepositoryId="$(cat releases/current-release-staging-repository-id)"
  exec_process "${dry_run}" ./gradlew releaseApacheStagingRepository --staging-repository-id "${stagingRepositoryId}" --stacktrace
else
  # Don't release anything when running locally, just print the statement
  exec_process 1 ./gradlew releaseApacheStagingRepository --staging-repository-id "<staging-repository-id>" --stacktrace
fi
end_group



start_group "Create Git tag ${new_tag_name}"
exec_process "${dry_run}" git tag "${new_tag_name}" "${from_tag_name}"
exec_process "${dry_run}" git push "${upstream_name}" "${new_tag_name}"
end_group



start_group "Generate release version docs"
cd "${worktree_dir}/site"
echo "Changed to directory $(pwd)"

echo "Checking out released version docs..."
bin/remove-releases.sh --force > /dev/null || true
bin/checkout-releases.sh

# Copy release docs content
rel_docs_dir="content/releases/${version_full}"
mkdir "${rel_docs_dir}"
echo "Copying docs from content/in-dev/unreleased to ${rel_docs_dir}..."
(cd content/in-dev/unreleased ; tar cf - .) | (cd "${rel_docs_dir}" ; tar xf -)

echo "Copying release notes file (potentially replacing existing one)"
cp "${worktree_dir}/releases/current-release-notes.md" "${rel_docs_dir}/release-notes.md"

# Copy release_index.md template as _index.md for the new release
versioned_docs_index_md="${rel_docs_dir}/_index.md"
echo "Updating released version ${versioned_docs_index_md}..."
had_marker=
while read -r ln ; do
  if [[ "${ln}" == "# RELEASE_INDEX_MARKER_DO_NOT_CHANGE" ]]; then
    had_marker=1
    cat << EOF
---
$(cat ../codestyle/copyright-header-hash.txt)
title: 'Polaris v${version_full}'
date: $(date --iso-8601=seconds)
params:
  release_version: "${version_full}"
cascade:
  exclude_search: false
EOF
  fi
  [[ ${had_marker} ]] && echo "${ln}"
done < content/in-dev/release_index.md > "${versioned_docs_index_md}"
end_group



start_group "Announcement email"
echo ""
echo "----------------------------------------------------------------------------------------------------------------"
echo ""
echo "Suggested announcement email subject:"
echo "====================================="
echo ""
echo "[ANNOUNCE] Apache Polaris release ${version_full}"
echo ""
echo ""
echo ""
echo "Suggested announcement email body:"
echo "=================================="
echo ""
cat << EOF
Hi everyone,

I'm pleased to announce the release of Apache Polaris ${version_full}!

Apache Polaris is an open-source, fully-featured catalog for Apache Iceberg™. It implements Iceberg's REST API,
enabling seamless multi-engine interoperability across a wide range of platforms, including Apache Doris™,
Apache Flink®, Apache Spark™, StarRocks, and Trino.

This release can be downloaded from: https://dlcdn.apache.org/polaris/apache-polaris-${version_full}/apache-polaris-${version_full}.tar.gz

Release notes at https://polaris.apache.org/release/${version_full}/release-notes
and https://github.com/apache/polaris/releases/tag/${new_tag_name}.

Docs for the ${version_full} release are on https://polaris.apache.org/release/${version_full}

Java artifacts are available from Maven Central.

Thanks to everyone for contributing!
EOF
echo ""
echo ""
echo "----------------------------------------------------------------------------------------------------------------"
echo ""
echo ""

cd content/releases
echo "Changed to directory $(pwd)"

exec_process "${dry_run}" git add .
exec_process "${dry_run}" git commit -m "Add versioned docs for release ${version_full}"
exec_process "${dry_run}" git push

cd "${worktree_dir}"
echo "Changed to directory $(pwd)"
end_group




start_group "GitHub release"
cd "${worktree_dir}"
if [[ ${CI} ]]; then
  exec_process "${dry_run}" gh release create "${new_tag_name}" \
    --notes-file "${version_full}/release-notes.md" \
    --title "Apache Polaris ${version_full}"
else
  # GitHub release only created from CI (dry-run always enabled locally)
  exec_process 1 gh release create "${new_tag_name}" \
    --notes-file "${version_full}/release-notes.md" \
    --title "Apache Polaris ${version_full}"
fi
end_group




echo ""
if [[ ${dry_run} ]]; then
  echo "*************************************"
  echo "Publish-release finished successfully - but dry run was enabled, no changes were made to Git or SVN"
  echo "*************************************"
else
  echo "*************************************"
  echo "Publish-release finished successfully"
  echo "*************************************"
fi
echo ""
echo ""
