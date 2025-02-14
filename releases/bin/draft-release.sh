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
    [--major MAJOR_VERSION] [--minor MINOR_VERSION] [--commit GIT_COMMIT]
    [--previous-version PREVIOUS_VERSION]
    [--recreate]
    [--dry-run]
    [--help | -h]

  Creates a release candidate.

  A new release candidate tag is created for the latest patch version for the major/minor version.
  If no RC tag (following the '${tag_prefix}<major>.<minor>.<patch>-rcx' pattern) exists, an RC1
  will be created, otherwise the RC number will be incremented. If the latest patch version is
  already promoted to GA, an RC1 for the next patch version will be created.

  Performs the Maven artifacts publication, Apache source tarball upload. Artifacts are signed, make
  sure to have a compatible GPG key present.

  Release notes are generated via the external generate-release-notes.sh script, which can also be
  invoked independently for development and testing purposes.

  Options:
    --major MAJOR_VERSION
        Major version number, must be specified when the command is called on the main branch.
    --minor MINOR_VERSION
        Minor version number, must be specified when the command is called on the main branch
        or on a major version branch.
    --commit GIT_COMMIT
        The Git commit to draft the release from. Defaults to the current HEAD.
    --previous-version PREVIOUS_VERSION
        The full (major.minor.patch) version to use as the base to collect commits and contributor
        information for the release notes file.
    --recreate
        Recreates the draft release if it already exists, to _replace_ an existing RC / reuse the RC iteration.
        This should only be used in exceptional cases and never in production.
    --dry-run
        Do not update Git. Gradle will publish to the local Maven repository, but still sign the artifacts.
    -h --help
        Print usage information.
EOF
}

dry_run=
recreate=
need_checkout=
new_major_version="${version_major}"
new_minor_version="${version_minor}"
create_from_commit="$(git rev-parse HEAD)"
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
    --commit)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Missing argument for --commit, aborting" > /dev/stderr
        exit 1
      fi
      create_from_commit="$1"
      need_checkout=1
      shift
      ;;
    --previous-version)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Missing argument for --previous-version, aborting" > /dev/stderr
        exit 1
      fi
      release_notes_previous_version_full="$1"
      shift
      ;;
    --recreate)
      recreate=1
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
case "${branch_type}" in
  "main")
    if [[ -z ${new_major_version} || -z ${new_minor_version} ]]; then
      echo "On the main branch, but specified no major and/or minor version using the '--major'/'--minor' arguments, aborting" > /dev/stderr
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

max_patch="$(get_max_patch_version ${new_major_version} ${new_minor_version})"
patch_version=
rc_iteration=
if [[ $max_patch -eq -1 ]]; then
  # No previous patch release
  patch_version=0
  rc_iteration=1
else
  max_rc="$(get_max_rc_iteration "${new_major_version}.${new_minor_version}.${max_patch}")"
  if [[ $max_rc -eq -2 ]]; then
    # that patch version is released, increase patch version
    patch_version="$(( $max_patch + 1))"
    rc_iteration=1
  elif [[ $max_rc -eq -1 ]]; then
    echo "Unexpected result -1 from get_next_rc_iteration function, aborting" > /dev/stderr
    exit 1
  else
    patch_version="${max_patch}"
    rc_iteration="$(( $max_rc + 1 ))"
  fi
fi

version_incubating="$(get_podling_version_suffix)"
version_full_base="${new_major_version}.${new_minor_version}.${patch_version}"
version_full="${version_full_base}${version_incubating}"
new_tag_name="${tag_prefix}${version_full_base}-rc${rc_iteration}"

echo ""
gradleDryRunSigning=""
gradleReleaseArgs=""
if [[ ${dry_run} ]]; then
  echo "Dry run, no changes will be made"

  # Only sign + use the GPG agent locally
  [[ ${CI} ]] || gradleDryRunSigning="-PsignArtifacts -PuseGpgAgent"
else
  echo "Non-dry run, will update Git"

  # Verify that the required secrets for the Maven publication are present.
  if [[ ${CI} ]]; then
    # Only publish to "Apache" from CI
    gradleReleaseArgs="-Prelease publishToApache closeApacheStagingRepository"

    if [[ -z ${ORG_GRADLE_PROJECT_signingKey} || -z ${ORG_GRADLE_PROJECT_signingPassword} || -z ${ORG_GRADLE_PROJECT_sonatypeUsername} || -z ${ORG_GRADLE_PROJECT_sonatypePassword} ]] ; then
      echo "One or more of the following required environment variables are missing:" > /dev/stderr
      [[ -z ${ORG_GRADLE_PROJECT_signingKey} ]] && echo "  ORG_GRADLE_PROJECT_signingKey" > /dev/stderr
      [[ -z ${ORG_GRADLE_PROJECT_signingPassword} ]] && echo "  ORG_GRADLE_PROJECT_signingPassword" > /dev/stderr
      [[ -z ${ORG_GRADLE_PROJECT_sonatypeUsername} ]] && echo "  ORG_GRADLE_PROJECT_sonatypeUsername" > /dev/stderr
      [[ -z ${ORG_GRADLE_PROJECT_sonatypePassword} ]] && echo "  ORG_GRADLE_PROJECT_sonatypePassword" > /dev/stderr
      exit 1
    fi
  else
    # Only publish to "Apache" from CI, otherwise publish to local Maven repo
    gradleReleaseArgs="-PsignArtifacts -PuseGpgAgent publishToMavenLocal"
  fi
fi

echo ""
echo "New version is:  ${version_full}"
echo "RC iteration:    ${rc_iteration}"
echo "New tag name is: ${new_tag_name}"
echo "From commit:     ${create_from_commit}"
echo ""
git log -n1 "${create_from_commit}"
echo ""

if [[ ${need_checkout} ]]; then
  exec_process 0 git checkout "${create_from_commit}"
fi
cleanups+=("git reset --hard HEAD")
cleanups+=("git checkout ${current_branch}")

if [[ -z ${new_tag_name} ]]; then
  echo "Empty tag to create - internal error, aborting" > /dev/stderr
  exit 1
fi

do_recreate=
if [[ ${recreate} ]]; then
  if list_release_tags "" "" | grep --quiet "${new_tag_name}"; then
    do_recreate=1
  fi
fi

cd "${worktree_dir}"
echo "Changed to directory $(pwd)"



start_group "Detach Git worktree"
git checkout "${create_from_commit}"
end_group



start_group "Update version.txt"
echo "Executing 'echo -n "${version_full}" > version.txt'"
echo -n "${version_full}" > version.txt
exec_process "${dry_run}" git add version.txt
end_group



start_group "Create release notes"
releaseNotesFile="releases/current-release-notes.md"
export SKIP_DIRTY_WORKSPACE_CHECK=42
echo "Release notes file: $releaseNotesFile"
echo "Executing 'releases/bin/generate-release-notes.sh --major ${new_major_version} --minor ${new_minor_version} --patch ${patch_version} --previous ${release_notes_previous_version_full}'"
"${worktree_dir}/releases/bin/generate-release-notes.sh" --major "${new_major_version}" --minor "${new_minor_version}" --patch "${patch_version}" --previous "${release_notes_previous_version_full}" > $releaseNotesFile
exec_process "${dry_run}" git add $releaseNotesFile
end_group



start_group "Gradle publication"
stagingRepositoryId="DRY RUN - NOTHING HAS BEEN STAGED - NO STAGING REPOSITORY ID!"
stagingRepositoryUrl="DRY RUN - NOTHING HAS BEEN STAGED - NO STAGING REPOSITORY URL!"
if [[ ${dry_run} ]]; then
  exec_process 0 ./gradlew publishToMavenLocal sourceTarball -PjarWithGitInfo ${gradleDryRunSigning} --stacktrace
else
  exec_process "${dry_run}" ./gradlew ${gradleReleaseArgs} sourceTarball --stacktrace | tee build/gradle-release-build.log
  if [[ ${CI} ]]; then
    # Extract staging repository ID from log (only do this in CI)
    # ... look for the log message similar to 'Created staging repository 'orgprojectnessie-1214' at https://oss.sonatype.org/service/local/repositories/orgprojectnessie-1214/content/'
    stagingLogMsg="$(grep 'Created staging repository' build/gradle-release-build.log)"
    stagingRepositoryId="$(echo $stagingLogMsg | sed --regexp-extended "s/^Created staging repository .([a-z0-9-]+). at (.*)/\1/")"
    stagingRepositoryUrl="$(echo $stagingLogMsg | sed --regexp-extended "s/^Created staging repository .([a-z0-9-]+). at (.*)/\2/")"
  fi
fi
# Memoize the commit-ID, staging-repository-ID+URL for later use and reference information
# The staging-repository-ID is required for the 'publish-release.sh' script to release the staging repository.
echo -n "${create_from_commit}" > releases/current-release-commit-id
echo -n "${stagingRepositoryId}" > releases/current-release-staging-repository-id
echo -n "${stagingRepositoryUrl}" > releases/current-release-staging-repository-url
exec_process "${dry_run}" git add releases/current-release-*
end_group



start_group "Validate source tarball"
cd "${worktree_dir}"
cd build/distributions/
shasum -a 512 -c "apache-polaris-$(cat "${worktree_dir}/version.txt").tar.gz.sha512"
end_group



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
    exec_process 0 svnadmin create "${svn_local_dummy_dev}"
  fi
  exec_process 0 svn checkout "file://${svn_local_dummy_dev}" .
else
  exec_process 0 svn checkout "${svn_dist_dev_repo}" .
fi

if [[ -d "${version_full}" ]]; then
  # Delete previous RC iterations
  exec_process 0 svn rm --force "${version_full}"
  exec_process 0 svn commit -m "Remove previous Polaris ${version_full} release candidate(s)"
fi
svn_rc_path="${version_full}/RC${rc_iteration}"
mkdir -p "${svn_rc_path}"

echo ""
find ../distributions/
echo ""

# We can safely assume that the Gradle 'sourceTarball' task leaves only the relevant files in
# build/distributions and that no other files are present
cp ../distributions/* "${svn_rc_path}"
exec_process 0 svn add --force "${version_full}" "${svn_rc_path}"
exec_process 0 svn commit -m "Polaris ${version_full} release candidate ${rc_iteration}"

cd "${worktree_dir}"
echo "Changed to directory $(pwd)"
end_group



start_group "Commit changes to Git"
exec_process "${dry_run}" git commit -m "[RELEASE] Version ${version_full}-rc${rc_iteration}

Base release commit ID: ${create_from_commit}
Staging repository ID: ${stagingRepositoryId}
Staging repository URL: ${stagingRepositoryUrl}
Release notes in this commit in file: ${releaseNotesFile}
"
tag_commit_id="$(git rev-parse HEAD)"
end_group



start_group "Create Git tag ${new_tag_name}"
[[ ${do_recreate} ]] && exec_process "${dry_run}" git tag -d "${new_tag_name}"
exec_process "${dry_run}" git tag "${new_tag_name}" "${tag_commit_id}"
end_group



start_group "Release vote email"
echo ""
echo "Suggested release vote email subject:"
echo "====================================="
echo ""
echo "[VOTE] Release Apache Polaris (Incubating) ${version_full}-rc${rc_iteration}"
echo ""
echo ""
echo ""
echo "Suggested Release vote email body:"
echo "=================================="
echo ""
cat << EOF
Hi everyone,

I propose that we release the following RC as the official
Apache Polaris (Incubating) ${version_full} release.

The commit ID is ${tag_commit_id}
* This corresponds to the tag: ${new_tag_name}
* https://github.com/apache/polaris/commits/${new_tag_name}
* https://github.com/apache/polaris/tree/${tag_commit_id}

The release tarball, signature, and checksums are here:
* ${svn_dist_dev_repo}/${svn_rc_path}/

You can find the KEYS file here:
* ${svn_dist_release_repo}/KEYS

Convenience binary artifacts are staged on Nexus. The Maven repository URL is:
* ${stagingRepositoryUrl}

Please download, verify, and test.

Please vote in the next 72 hours.

[ ] +1 Release this as Apache Polaris ${version_full}
[ ] +0
[ ] -1 Do not release this because...

Only PPMC members and mentors have binding votes, but other community members
are encouraged to cast non-binding votes. This vote will pass, if there are
3 binding +1 votes and more binding +1 votes than -1 votes.

NB: if this vote pass, a new vote has to be started on the Incubator general mailing
list.

Thanks
Regards
EOF
echo ""
echo ""
end_group



start_group "Push Git tag ${new_tag_name}"
[[ ${do_recreate} ]] && exec_process "${dry_run}" git push "${upstream_name}" --delete "${new_tag_name}"
exec_process "${dry_run}" git push "${upstream_name}" "${new_tag_name}"
end_group



echo ""
if [[ ${dry_run} ]]; then
  echo "***********************************"
  echo "Draft-release finished successfully - but dry run was enabled, no changes were made to Git or SVN"
  echo "***********************************"
else
  echo "***********************************"
  echo "Draft-release finished successfully"
  echo "***********************************"
fi
echo ""
echo ""
