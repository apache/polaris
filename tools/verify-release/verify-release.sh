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

maven_repo_url_prefix="https://repository.apache.org/content/repositories/orgapachepolaris-"

function usage() {
  cat << ! > /dev/stderr

Apache Polaris release candidate verification tool.

Usage: $0 [options]

  Mandatory options:
    -s | --git-sha | --sha <GIT_SHA>   Git commit (full, not abbreviated)
                                       Example: b7188a07511935e7c9c64128dc047107c26f97f6
    -v | --version <version>           Release version, including '-incubating' and RC
                                       Example: 1.3.0-incubating-rc0
    -m | --maven-repo-id <staging-ID>  Staging Maven repository staging ID
                                       Example: 1234
                                       This will be prefixed with ${maven_repo_url_prefix}

  Optional arguments:
    -k | --keep-temp-dir               Keep the temporary directory (default is to purge it once the script exits)
    -h | --help                        Show usage information (exits early)


Full example for RC4 of version 9.8.0, staging repo ID 4568.
  ./verify-release.sh -s b7188a07511935e7c9c64128dc047107c26f97f6 -v 9.8.0-incubating-rc4 -m 4568
!
}

git_sha=""
version=""
maven_repo_id=""
keep_temp_dir=0

while [[ $# -gt 0 ]]; do
  arg="$1"
  case "$arg" in
  -s | --git-sha | --sha)
    git_sha="$2"
    shift
    ;;
  -v | --version)
    version="$2"
    shift
    ;;
  -m | --maven-repo-id)
    maven_repo_id="$2"
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  -k | --keep-temp-dir)
    keep_temp_dir=1
    ;;
  *)
    usage
    exit 1
    ;;
  esac
  shift
done

RED='\033[0;31m'
ORANGE='\033[0;33m'
RESET='\033[m'

# Allow leading characters, but only extract the version and rc.
VERSION_RC_REGEX="^([a-z-]+)?([0-9]+\.[0-9]+\.[0-9]+(-incubating)?)-rc([0-9]+)$"
if [[ ! ${version} =~ ${VERSION_RC_REGEX} ]]; then
  echo "Version '${version}' does not match the version pattern 0.0.0-incubating-rc0" > /dev/stderr
  exit 1
fi
version="${BASH_REMATCH[2]}"
rc_num="${BASH_REMATCH[4]}"

# Allow leading characters for convenience, but extract the staging repo number.
STAGING_REGEX="[a-z-]*([0-9]+)$"
if [[ ! ${maven_repo_id} =~ ${STAGING_REGEX} ]]; then
  echo "Invalid Maven staging repo ID '${maven_repo_id}'" > /dev/stderr
  exit 1
fi
maven_repo_id="${BASH_REMATCH[1]}"

fatal_reported=0
finished=0

run_id="polaris-release-verify-$(date "+%Y-%m-%d-%H-%M-%S")"
# Note mktemp/GNU yields no trailing slash, mktemp/BSD does yield a trailing slash.
# The difference is not an issue though.
temp_dir="$(mktemp --tmpdir --directory "${run_id}-XXXXXXXXX")"
function purge_temp_dir {
  if [[ $keep_temp_dir -eq 0 ]] ; then
    if [[ $fatal_reported -eq 0 && $finished -eq 1 ]] ; then
      echo "Purging ${temp_dir}..."
      rm -rf "${temp_dir}"
    else
      echo "⚠ NOT purging temporary directory ${temp_dir} - errors were reported!"
    fi
  else
    echo "Leaving ${temp_dir} around, you may want to purge it."
  fi
}
trap purge_temp_dir EXIT

dist_dir="${temp_dir}/dist"
helm_dir="${temp_dir}/helm"
helm_work_dir="${temp_dir}/helm_work"
worktree_dir="${temp_dir}/worktree"
maven_repo_dir="${temp_dir}/maven-repo"
maven_local_dir="${temp_dir}/maven-local"
keys_file="${temp_dir}/KEYS"
gpg_keyring="${temp_dir}/keyring.gpg"

failures_file="$(pwd)/${run_id}.log"

dist_url_prefix="https://dist.apache.org/repos/dist/dev/incubator/polaris/"
keys_file_url="https://downloads.apache.org/incubator/polaris/KEYS"

git_tag_full="apache-polaris-${version}-rc${rc_num}"

GITHUB=0
[[ -n ${GITHUB_ENV} ]] && GITHUB=1

# Common excludes for "find'
find_excludes=(
  # Exclude GPG signatures and checksums
  '!' '-name' '*.asc'
  '!' '-name' '*.md5'
  '!' '-name' '*.sha1'
  '!' '-name' '*.sha256'
  '!' '-name' '*.sha512'
  # file with that name is created by wget when mirroring from 'dist'
  '!' '-name' "${version}"
  # ignore Maven repository metadata
  '!' '-name' 'maven-metadata*.xml'
  '!' '-name' 'archetype-catalog.xml'
)

dist_url="${dist_url_prefix}${version}"
helm_url="${dist_url_prefix}helm-chart/${version}"
maven_repo_url="${maven_repo_url_prefix}${maven_repo_id}"

function log_part_start {
  local heading
  local separator
  heading="${*}"
  [ ${GITHUB} == 1 ] && echo "::group::$heading"
  echo ""
  # shellcheck disable=SC2046
  separator="--$(printf -- '-%0.s' $(eval "echo {1..${#heading}}"))--"
  echo "${separator}"
  echo "  ${heading}"
  echo "${separator}"
}

function log_part_end {
  [ ${GITHUB} == 1 ] && echo "::endgroup::"
  echo ""
}

function log_fatal {
  fatal_reported=1
  echo -n -e "❌ ${RED}"
  echo -n "$1"
  echo -e "${RESET}"
  echo "" >> "${failures_file}"
  for i in "${@}"; do
    echo "$i" >> "${failures_file}"
  done
}

function log_warn {
  echo -ne "⚠ ${ORANGE}"
  echo -n "$1"
  echo -e "${RESET}"
}

function log_info {
  echo "$1"
}

# Executes a process and captures a fatal error if the process did not complete successfully.
# The full output of the process execution will be logged in the FAILURES file, but not printed to the console.
#   First argument: log message for 'log_fatal'
#   Following arguments: process arguments
function proc_exec {
  local err_msg
  local output
  err_msg=$1
  shift
  output=()
  IFS=$'\n' read -r -d '' -a output < <( "${@}" 2>&1 && printf '\0' ) || (
    log_fatal "${err_msg}" "${output[@]}"
    return 1
  )
}

function mirror {
  local url
  local dir
  local cut
  local wget_executable
  url="$1"
  dir="$2"
  cut="$3"
  wget_executable="wget"
  # Prefer wget2 as it allows parallel downloads (wget does not)
  (which wget2 > /dev/null) && wget_executable="wget2 --max-threads=8"
  log_part_start "Mirroring $url, this may take a while..."
  mkdir -p "${dir}"
  (cd "${dir}" ; ${wget_executable} \
    --no-parent \
    --no-verbose \
    --no-host-directories \
    --mirror \
    -e robots=off \
    --cut-dirs="${cut}" \
    "${url}/")
  # Nuke the directory listings (index.html from server) and robots.txt...
  # (only wget2 downloads the robots.txt :( )
  find "${dir}" \( -name index.html -o -name robots.txt \) -exec rm {} +
  # The following is a hack for wget2, which behaves a bit different than wget.
  # If the server returns `Content-Type: application/x-gzip`, the file is stored gzipped,
  # although it's "plain text". Leaving it as gzip breaks signature + checksum tests.
  find "${dir}" -name "*.prov" | while read -r helmProv; do
    if gunzip -c "${helmProv}" > /dev/null 2>&1 ; then
      mv "${helmProv}" "${helmProv}.gz"
      gunzip "${helmProv}.gz"
    fi
  done || log_fatal "find failed, please try again"
  log_part_end
}

function verify_checksums {
  local dir
  dir="$1"
  log_part_start "Verifying signatures and checksums in ${dir} ..."
  find "${dir}" -mindepth 1 -type f "${find_excludes[@]}" | while read -r fn ; do
    echo -n ".. $fn ... "
    if [[ -f "$fn.asc" ]] ; then
      echo -n "sig "
      proc_exec "$fn : Invalid signature" gpg --no-default-keyring --keyring "${gpg_keyring}" --verify "$fn.asc" "$fn" || true
    else
      log_fatal "$fn : Mandatory ASC signature missing"
    fi
    if [[ -f "$fn.sha512" ]] ; then
      echo -n "sha512 "
      provided="$(cut -d\  -f1 < "$fn.sha512")" || log_fatal "sha512 provided $fn failed"
      calc="$($sha512_exec -b "$fn" | cut -d\  -f1)" || log_fatal "sha512 calc $fn failed"
      [[ "$provided" != "$calc" ]] && log_fatal "$fn : Expected SHA512 $calc - provided $provided"
    else
      log_fatal "$fn : Mandatory SHA512 missing"
    fi
    if [[ -f "$fn.sha256" ]] ; then
      echo -n "sha256 "
      provided="$(cut -d\  -f1 < "$fn.sha256")" || log_fatal "sha256 provided $fn failed"
      calc="$($sha256_exec -b "$fn" | cut -d\  -f1)" || log_fatal "sha256 calc $fn failed"
      [[ "$provided" != "$calc" ]] && log_fatal "$fn : Expected SHA256 $calc - provided $provided"
    fi
    if [[ -f "$fn.sha1" ]] ; then
      echo -n "sha1 "
      provided="$(cut -d\  -f1 < "$fn.sha1")" || log_fatal "sha1 provided $fn failed"
      calc="$($sha1_exec -b "$fn" | cut -d\  -f1)" || log_fatal "sha1 calc $fn failed"
      [[ "$provided" != "$calc" ]] && log_fatal "$fn : Expected SHA1 $calc - provided $provided"
    fi
    if [[ -f "$fn.md5" ]] ; then
      echo -n "md5 "
      provided="$(cut -d\  -f1 < "$fn.md5")" || log_fatal "md5 provided $fn failed"
      calc="$(md5sum -b "$fn" | cut -d\  -f1)" || log_fatal "md5 calc $fn failed"
      [[ "$provided" != "$calc" ]] && log_fatal "$fn : Expected MD5 $calc - provided $provided"
    fi
    echo ""
  done || log_fatal "find failed, please try again"
  log_part_end
}

function report_mismatch {
  local local_file
  local repo_file
  local title
  local_file="$1"
  repo_file="$2"
  title="$3"
  case "${local_file}" in
  *.jar | *.zip)
    if proc_exec "${title}" zipcmp "${local_file}" "${repo_file}"; then
      # Dump ZIP info only when the contents are equal (to inspect mtime and posix attributes)
      (
        log_warn "${title}"
        echo ">>>>>>>>>>>>>>> zipinfo ${local_file}"
        zipinfo "${local_file}" || true
        echo ">>>>>>>>>>>>>>> zipinfo ${repo_file}"
        zipinfo "${repo_file}" || true
        echo ""
      ) >> "${failures_file}"
    fi
    ;;
  *.tar.gz | *.tgz | *.tar)
    mkdir "${local_file}.extracted" "${repo_file}.extracted"
    tar ${tar_opts} -xf "${local_file}" --directory "${local_file}.extracted" || true
    log_info "  extracted to ${local_file}.extracted"
    tar ${tar_opts} -xf "${repo_file}" --directory "${repo_file}.extracted" || true
    log_info "  extracted to ${repo_file}.extracted"
    if proc_exec "${title}" diff --recursive "${local_file}.extracted" "${repo_file}.extracted"; then
      # Dump tar listing only when the contents are equal (to inspect mtime and posix attributes)
      log_warn "${title}"
      (
        echo "${title}" # log_warn above prints ANSI escape sequences
        tar ${tar_opts} -tvf "${local_file}" > "${local_file}.contents-listing" || true
        tar ${tar_opts} -tvf "${repo_file}" > "${repo_file}.contents-listing" || true
        echo "Diff of archives contents:"
        diff "${local_file}.contents-listing" "${repo_file}.contents-listing"
        echo ">>>>>>>>>>>>>>> tar tvf ${local_file}"
        cat "${local_file}.contents-listing"
        echo ">>>>>>>>>>>>>>> tar tvf ${repo_file}"
        cat "${repo_file}.contents-listing"
        echo ""
      ) >> "${failures_file}"
    fi
    ;;
  *)
    log_fatal "${title}"
    (
      diff "${local_file}" "${repo_file}" || true
      echo ""
    ) >> "${failures_file}"
    ;;
  esac
}

function compare_binary_file {
  local name
  local filename
  local local_file
  local repo_file
  name="$1"
  filename="$2"
  local_file="$3/${filename}"
  repo_file="$4/${filename}"
  if ! diff "${local_file}" "${repo_file}" > /dev/null ; then
    report_mismatch "${local_file}" "${repo_file}" "Locally built and staged $name $filename differ"
  fi
}

missing_tools=()
for mandatory_tool in wget gunzip find git helm java gpg md5sum shasum tar curl zipcmp zipinfo ; do
  if ! which "${mandatory_tool}" > /dev/null; then
    missing_tools+=("${mandatory_tool}")
  fi
done
if [[ ${#missing_tools} -ne 0 ]]; then
  log_fatal "Mandatory tools ${missing_tools[*]} are missing, please install those first."
  log_info " Note for macOS: zipcmp can be installed with brew via libzip"
  exit 1
fi
if ! which wget2 > /dev/null; then
  log_warn "For improved website mirroring performance consider installing 'wget2' as it allows multi-threaded downloads."
  log_warn "  wget2 however may print misleading warnings like 'Failed to parse date '"
fi

# Prefer the native tools, if available. It doesn't matter on Linux, but it's important for macOS as shasum,
# implemented in Perl, can cause problems surfacing as a Perl compilation issue:
#   Can't locate warnings/register.pm in @INC (you may need to install the warnings::register module) (@INC contains: /Library/Perl/5.34/darwin-thread-multi-2level /Library/Perl/5.34 /Network/Library/Perl/5.34/darwin-thread-multi-2level /Network/Library/Perl/5.34 /Library/Perl/Updates/5.34.1 /System/Library/Perl/5.34/darwin-thread-multi-2level /System/Library/Perl/5.34 /System/Library/Perl/Extras/5.34/darwin-thread-multi-2level /System/Library/Perl/Extras/5.34) at /System/Library/Perl/5.34/vars.pm line 7.
#   BEGIN failed--compilation aborted at /System/Library/Perl/5.34/vars.pm line 7.
#   Compilation failed in require at /System/Library/Perl/5.34/Getopt/Long.pm line 20.
#   BEGIN failed--compilation aborted at /System/Library/Perl/5.34/Getopt/Long.pm line 20.
#   Compilation failed in require at /usr/bin/shasum line 23.
#   BEGIN failed--compilation aborted at /usr/bin/shasum line 23
sha256_exec="shasum -a 256"
which sha256sum > /dev/null && sha256_exec=sha256sum
sha512_exec="shasum -a 512"
which sha512sum > /dev/null && sha512_exec=sha512sum
sha1_exec="shasum -a 1"
which sha1sum > /dev/null && sha1_exec=sha1sum

if [[ -z $git_sha || -z $version || -z $rc_num || -z $maven_repo_id ]]; then
  echo "Mandatory parameter missing" > /dev/stderr
  usage
  exit 1
fi

tar_opts=""
[[ $OSTYPE == "linux-gnu" ]] && tar_opts="--warning=no-timestamp"

touch "${failures_file}"

cat << !

Verifying staged release
========================

Git tag:           ${git_tag_full}
Git sha:           ${git_sha}
Version:           ${version}
RC:                ${rc_num}
Maven repo URL:    ${maven_repo_url}
Main dist URL:     ${dist_url}
Helm chart URL:    ${helm_url}
Verify directory:  ${temp_dir}

A verbose log containing the identified issues will be available here:
    ${failures_file}

!

log_part_start "Create verification keyring from ${keys_file_url} ..."
curl --silent --output "${keys_file}" "${keys_file_url}"
gpg --no-default-keyring --keyring "${gpg_keyring}" --import "${keys_file}"
log_part_end

# Git dance:
# Fetch from remotes and create a local Git worktree for the RC tag, verify Git SHAs.
log_part_start "Git checkout tag ${git_tag_full}, expecting ${git_sha}"
mkdir -p "${worktree_dir}"
(cd "${worktree_dir}"
  proc_exec "git init failed" git init . 2> /dev/null
  proc_exec "git config failed" git config --local gc.auto 0
  proc_exec "git remote add failed " git remote add origin https://github.com/apache/polaris.git
  proc_exec "git fetch failed" git fetch origin tag "${git_tag_full}"
  proc_exec "git checkout failed" git checkout "${git_tag_full}"
)
git_sha_on_tag="$(cd "${worktree_dir}" ; git rev-parse HEAD)"
log_info "Git commit on tag '${git_tag_full}':   ${git_sha_on_tag}"
if [[ "$git_sha_on_tag" != "$git_sha" ]]; then
  log_fatal "Expected Git SHA ${git_sha} is different from the current SHA ${git_sha_on_tag}"
fi
log_part_end

log_part_start "Verify mandatory files in source tree"
  [[ -e "${worktree_dir}/DISCLAIMER" ]] || log_fatal "Mandatory DISCLAIMER file missing in source tree"
  [[ -e "${worktree_dir}/LICENSE" ]] || log_fatal "Mandatory LICENSE file missing in source tree"
  [[ -e "${worktree_dir}/NOTICE" ]] || log_fatal "Mandatory NOTICE file missing in source tree"
  [[ "$(cat "${worktree_dir}/version.txt")" == "${version}" ]] || log_fatal "version.txt in source tree does not contain expected version"
log_part_end

# Mirror the helm chart content for the release, verify signatures and checksums
mirror "${helm_url}" "${helm_dir}" 7
verify_checksums "${helm_dir}"
# Mirror the main distribution content for the release, verify signatures and checksums
mirror "${dist_url}" "${dist_dir}" 6
verify_checksums "${dist_dir}"
# Mirror the repository.apache.org content for the release, verify signatures and checksums
mirror "${maven_repo_url}" "${maven_repo_dir}" 3
verify_checksums "${maven_repo_dir}"

# Build Polaris ("assemble") and publish to a separate local Maven repository
log_part_start "Building Polaris ..."
mkdir -p "${maven_local_dir}"
(cd "${worktree_dir}" ; ./gradlew \
  -Dmaven.repo.local="${maven_local_dir}" \
  publishToMavenLocal \
  sourceTarball \
  assemble \
  -PjarWithGitInfo
)
log_part_end

# Check that the the set of locally built Maven artifacts and staged Maven artifacts is the same.
log_part_start "Comparing Maven build artifacts ..."
(cd "${maven_local_dir}" ; find . -mindepth 1 -type f "${find_excludes[@]}" -print) \
  | sort \
  > "${temp_dir}/maven-local-files"
(cd "${maven_repo_dir}" ; find . -mindepth 1 -type f "${find_excludes[@]}" -print) \
  | sort \
  > "${temp_dir}/maven-repo-files"
proc_exec "List of locally build Maven artifacts and staged artifacts differs!" \
  diff "${temp_dir}/maven-local-files" "${temp_dir}/maven-repo-files" || true
log_part_end

# Verify that the locally built Maven artifacts are reproducible (binary equal)
log_part_start "Comparing Maven repository artifacts, this will take a little while..."
while read -r fn ; do
  compare_binary_file "Maven repository artifact" "${fn}" "${maven_local_dir}" "${maven_repo_dir}"
  # verify that the "main" and sources jars contain LICENSE + NOTICE files
  [[ "${fn}" =~ .*-$version(-sources)?[.]jar ]] && (
    if [[ $(zipinfo -1 "${maven_repo_dir}/${fn}" | grep --extended-regexp --count "^META-INF/(LICENSE|NOTICE)$") -ne 2 ]] ; then
      log_fatal "${fn}: Mandatory LICENSE/NOTICE files not in META-INF/"
    fi
  )
done < "${temp_dir}/maven-local-files"
log_part_end

log_part_start "Comparing main distribution artifacts"
compare_binary_file "source tarball" "apache-polaris-${version}.tar.gz" "${worktree_dir}/build/distributions" "${dist_dir}"
dist_file_prefix="polaris-bin-${version}"
compare_binary_file "Polaris distribution tarball" "${dist_file_prefix}.tgz" "${worktree_dir}/runtime/distribution/build/distributions" "${dist_dir}"
if [[ $(tar -tf "${dist_dir}/${dist_file_prefix}.tgz" | grep --extended-regexp --count "^${dist_file_prefix}/(DISCLAIMER|LICENSE|NOTICE)$") -ne 3 ]] ; then
  log_fatal "${dist_file_prefix}.tgz: Mandatory DISCLAIMER/LICENSE/NOTICE files not in ${dist_file_prefix}/"
fi
compare_binary_file "Polaris distribution zip" "${dist_file_prefix}.zip" "${worktree_dir}/runtime/distribution/build/distributions" "${dist_dir}"
if [[ $(zipinfo -1 "${dist_dir}/${dist_file_prefix}.zip" | grep --extended-regexp --count "^${dist_file_prefix}/(DISCLAIMER|LICENSE|NOTICE)$") -ne 3 ]] ; then
  log_fatal "${dist_file_prefix}.zip: Mandatory DISCLAIMER/LICENSE/NOTICE files not in ${dist_file_prefix}/"
fi
log_part_end

log_part_start "Comparing helm chart artifacts"
mkdir -p "${helm_work_dir}/local" "${helm_work_dir}/staged"
# Prerequisite for reproducible helm packages: file modification time must be deterministic
# Works with helm since version 4.0.0
find "${worktree_dir}/helm/polaris" -exec touch -d "1980-01-01 00:00:00" {} +
proc_exec "Helm packaging failed" helm package --destination "${helm_work_dir}" "${worktree_dir}/helm/polaris"
helm_package_file="polaris-${version}.tgz"
tar ${tar_opts} -xf "${helm_dir}/${helm_package_file}" --directory "${helm_work_dir}/staged" || true
tar ${tar_opts} -xf "${helm_work_dir}/${helm_package_file}" --directory "${helm_work_dir}/local" || true
proc_exec "Helm package ${helm_package_file} contents" diff -r "${helm_work_dir}/local" "${helm_work_dir}/staged"
[[ -e "${helm_work_dir}/staged/polaris/DISCLAIMER" ]] || log_fatal "Mandatory DISCLAIMER file missing in Helm package ${helm_package_file}"
[[ -e "${helm_work_dir}/staged/polaris/LICENSE" ]] || log_fatal "Mandatory LICENSE file missing in Helm package ${helm_package_file}"
[[ -e "${helm_work_dir}/staged/polaris/NOTICE" ]] || log_fatal "Mandatory NOTICE file missing in Helm package ${helm_package_file}"
log_part_end

if [[ -s ${failures_file} ]] ; then
  cat << !

************************************************************************************************************
** Automatic release check FAILED !
************************************************************************************************************

One or more staged release artifacts did not pass the required checks.
A detailed report is available in the file

  ${failures_file}

INSPECT THE CONTENTS OF THE ABOVE FILE _BEFORE_ REPORTING THE RELEASE CONTENTS AS INVALID!

* Git SHA mismatches MUST be treated as fatal.
* GPG signature verification errors MUST be treated as fatal.
* Checksum mismatches MUST be treated as fatal.
* Files being reported as missing MUST be treated as fatal.

The Polaris build is not yet fully reproducible.
A list of known reproducible build issues is maintained in https://github.com/apache/polaris/issues/2204.

Pending on full support for reproducible builds in Quarkus:
* Jars containing generated code are not guaranteed to be reproducible. Affects the following jars:
  * */quarkus/generated-bytecode.jar
  * */quarkus/transformed-bytecode.jar
  * */quarkus/quarkus-application.jar
* Re-assembled jars are not guaranteed to be reproducible: Affects the following jars:
  * admin/app/polaris-admin-*.jar
  * server/app/polaris-server-*.jar
* Zips and tarballs containing any of the above are not guaranteed to be reproducible.

!
  exit 1
else
  cat << !
************************************************************************************************************
** Automatic release check succeeded
************************************************************************************************************

None of the implemented automatic staged release checks reported a mismatch or failure.
* The source tarball matches the contents at the referenced Git commit.
* GPG signatures and checksums are valid and correct.
* The locally built release artifacts are binary equal to the staged release artifacts.

The contents of all LICENSE and NOTICE files however MUST be verified manually.

!
fi

finished=1
