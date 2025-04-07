#!/usr/bin/env bats
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

setup() {
  load 'test_helper/bats-support/load'
  load 'test_helper/bats-assert/load'
  load 'test_helper/bats-file/load'

  # make the tested stuff visible to PATH
  RELEASE_BIN="${BATS_TEST_DIRNAME}/../bin"
  PRJ_ROOT="${BATS_TEST_DIRNAME}/../.."
  PATH="${RELEASE_BIN}:$PATH"

  # shellcheck disable=SC2034
  svn_dir_prefix="${BATS_RUN_TMPDIR}/svn-dir"
}

@test "draft-release help" {
  run git status --untracked-files=no --porcelain
  assert_output ""

  run draft-release.sh -h
  assert_line "Version in version.txt is '$(cat "$PRJ_ROOT"/version.txt)'"
  assert_line --partial "Current branch is '"
  assert_line --partial "Release Git remote name '"
  assert_success
}

@test "draft-release dry-run" {
  if [[ -n ${CI} ]]; then
    skip "Test requires GPG signing, not available in CI"
  fi

  run git status --untracked-files=no --porcelain
  assert_output ""

  run draft-release.sh \
    --major 42 --minor 43 --label bats-testing \
    --previous-version 0.9.0-incubating \
    --commit 68fb898dfd9950cb312a26e923ab0f4e4f0cb6dc \
    --dry-run \
    --keep-release-worktree

  assert_line --partial "Version in version.txt is '"

  assert_line "Dry run, no changes will be made"

  assert_line "New version is:  42.43.0-incubating-bats-testing"
  assert_line "RC iteration:    1"
  assert_line "New tag name is: apache-polaris-42.43.0-incubating-bats-testing-rc1"
  assert_line "From commit:     68fb898dfd9950cb312a26e923ab0f4e4f0cb6dc"

  assert_line --partial "Changed to directory "

  # Update version.txt
  assert_line "Executing 'echo -n \"42.43.0-incubating-bats-testing\" > version.txt'"
  assert_line "Executing 'git add version.txt'"

  # Create release notes
  assert_line "Release notes file: releases/current-release-notes.md"
  assert_line "Executing 'releases/bin/generate-release-notes.sh --major 42 --minor 43 --patch 0 --label -incubating-bats-testing --previous 0.9.0-incubating'"
  assert_line "Executing 'git add releases/current-release-notes.md'"

  # Gradle publication
  assert_line "Executing './gradlew clean publishToMavenLocal sourceTarball -PjarWithGitInfo -PsignArtifacts -PuseGpgAgent --stacktrace'"

  # Validate source tarball
  assert_line "apache-polaris-42.43.0-incubating-bats-testing.tar.gz: OK"

  # Upload source tarball
  assert_line "Executing 'svn add --force 42.43.0-incubating-bats-testing 42.43.0-incubating-bats-testing/RC1'"
  assert_line "A         42.43.0-incubating-bats-testing"
  assert_line "A         42.43.0-incubating-bats-testing/RC1"
  assert_line "A  (bin)  42.43.0-incubating-bats-testing/RC1/apache-polaris-42.43.0-incubating-bats-testing.tar.gz"
  assert_line "A         42.43.0-incubating-bats-testing/RC1/apache-polaris-42.43.0-incubating-bats-testing.tar.gz.sha512"
  assert_line "A         42.43.0-incubating-bats-testing/RC1/apache-polaris-42.43.0-incubating-bats-testing.tar.gz.asc"
  assert_line "Executing 'svn commit -m Polaris 42.43.0-incubating-bats-testing release candidate 1'"
  assert_line "Adding         42.43.0-incubating-bats-testing"
  assert_line "Adding         42.43.0-incubating-bats-testing/RC1"
  assert_line "Adding  (bin)  42.43.0-incubating-bats-testing/RC1/apache-polaris-42.43.0-incubating-bats-testing.tar.gz"
  assert_line "Adding         42.43.0-incubating-bats-testing/RC1/apache-polaris-42.43.0-incubating-bats-testing.tar.gz.asc"
  assert_line "Adding         42.43.0-incubating-bats-testing/RC1/apache-polaris-42.43.0-incubating-bats-testing.tar.gz.sha512"

  # Commit changes to Git
  assert_line "Executing 'git commit -m [RELEASE] Version 42.43.0-incubating-bats-testing-rc1"

  # Create Git tag apache-polaris-42.43.0-incubating-bats-testing-rc1
  assert_line --partial "Dry-run, WOULD execute 'git tag apache-polaris-42.43.0-incubating-bats-testing-rc1 "

  # Release vote mail subject
  assert_line "[VOTE] Release Apache Polaris (Incubating) 42.43.0-incubating-bats-testing-rc1"
  # Release vote mail content
  assert_line "* This corresponds to the tag: apache-polaris-42.43.0-incubating-bats-testing-rc1"
  assert_line "[ ] +1 Release this as Apache Polaris 42.43.0-incubating-bats-testing"
  assert_success

  cd "${PRJ_ROOT}"/build/releases-git-worktree
  run git log -n1

  assert_line "    [RELEASE] Version 42.43.0-incubating-bats-testing-rc1"
  assert_line "    Base release commit ID: 68fb898dfd9950cb312a26e923ab0f4e4f0cb6dc"
  assert_line "    Staging repository ID: DRY RUN - NOTHING HAS BEEN STAGED - NO STAGING REPOSITORY ID!"
  assert_line "    Staging repository URL: DRY RUN - NOTHING HAS BEEN STAGED - NO STAGING REPOSITORY URL!"
  assert_line "    Release notes in this commit in file: releases/current-release-notes.md"
  assert_line --partial "    Release scripts version: "

  assert_file_contains releases/current-release-notes.md "New commits in version 42.43.0-incubating-bats-testing since 0.9.0-incubating"
  assert_file_contains releases/current-release-commit-id "68fb898dfd9950cb312a26e923ab0f4e4f0cb6dc"
  assert_file_not_empty releases/current-release-staging-repository-id
  assert_file_not_empty releases/current-release-staging-repository-url
}
