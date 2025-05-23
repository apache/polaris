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
}

# shellcheck disable=SC2034
@test "get_podling_version_suffix" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_lib.sh"
  run get_podling_version_suffix
  assert_output "-incubating"
  assert_success
}

# shellcheck disable=SC2034
@test "list_release_branches" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_releases_lib.sh"
  run list_release_branches "" ""
  assert_line "release/0.9.x"
  assert_success
}

# shellcheck disable=SC2034
@test "list_release_tags no prefix" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_releases_lib.sh"
  run list_release_tags ""
  assert_line "apache-polaris-0.9.0-incubating"
  assert_line "apache-polaris-0.9.0-incubating-rc1"
  assert_line "apache-polaris-0.9.0-incubating-rc6"
  assert_success
}

# shellcheck disable=SC2034
@test "major_minor_version_from_branch_name" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_releases_lib.sh"

  run major_version_from_branch_name "release/0.9.x"
  assert_output "0"
  assert_success

  run minor_version_from_branch_name "release/0.9.x"
  assert_output "9"
  assert_success

  run major_version_from_branch_name "release/0.9"
  assert_output "0"
  assert_success

  run minor_version_from_branch_name "release/0.9"
  assert_output "9"
  assert_success

  run major_version_from_branch_name "release/1.2.x"
  assert_output "1"
  assert_success

  run minor_version_from_branch_name "release/1.2.x"
  assert_output "2"
  assert_success

  run major_version_from_branch_name "release/1.2"
  assert_output "1"
  assert_success

  run minor_version_from_branch_name "release/1.2"
  assert_output "2"
  assert_success
}

# shellcheck disable=SC2034
@test "get_max_rc_iteration" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_releases_lib.sh"

  run _get_max_rc_iteration << EOF
apache-polaris-0.9.0-incubating-rc1
apache-polaris-0.9.0-incubating-rc2
apache-polaris-0.9.0-incubating-rc3
EOF
  assert_output "3"
  assert_success

  run _get_max_rc_iteration << EOF
apache-polaris-0.9.0-incubating
apache-polaris-0.9.0-incubating-rc1
apache-polaris-0.9.0-incubating-rc2
apache-polaris-0.9.0-incubating-rc3
EOF
  assert_output "-2" # marker, that 0.9.0-incubating is released
  assert_success
}

# shellcheck disable=SC2034
@test "get_max_patch_version" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_releases_lib.sh"

  run _get_max_patch_version << EOF
apache-polaris-0.42.0-incubating
apache-polaris-0.42.0-incubating-rc1
apache-polaris-0.42.7-incubating
apache-polaris-0.42.7-incubating-rc1
apache-polaris-0.42.7-incubating-rc2
apache-polaris-0.42.3-incubating
apache-polaris-0.42.3-incubating-rc1
apache-polaris-0.42.3-incubating-rc2
EOF
  assert_output "7"
  assert_success
}

# shellcheck disable=SC2034
@test "tag_for_full_version" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_releases_lib.sh"

  run _tag_for_full_version << EOF
apache-polaris-0.42.0-incubating
apache-polaris-0.42.0-incubating-rc1
apache-polaris-0.42.0-incubating-rc3
EOF
  assert_output "apache-polaris-0.42.0-incubating"
  assert_success

  run _tag_for_full_version << EOF
apache-polaris-0.42.0-incubating-rc3
apache-polaris-0.42.0-incubating
apache-polaris-0.42.0-incubating-rc1
EOF
  assert_output "apache-polaris-0.42.0-incubating"
  assert_success

  run _tag_for_full_version << EOF
apache-polaris-0.42.0-incubating
apache-polaris-0.42.0-incubating-rc1
apache-polaris-0.42.0-incubating-rc3
EOF
  assert_output "apache-polaris-0.42.0-incubating"
  assert_success
}

# shellcheck disable=SC2034
@test "get_latest_rc_tag_name" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_releases_lib.sh"

  run _get_latest_rc_tag_name << EOF
apache-polaris-0.42.3-incubating-rc1
apache-polaris-0.42.3-incubating-rc13
EOF
  assert_output "apache-polaris-0.42.3-incubating-rc13"
  assert_success

  run _get_latest_rc_tag_name << EOF
apache-polaris-0.42.3-incubating
apache-polaris-0.42.3-incubating-rc1
apache-polaris-0.42.3-incubating-rc13
EOF
  assert_output ""
  assert_success
}

# shellcheck disable=SC2034
@test "full_version_with_label_from_release_tag" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_releases_lib.sh"

  run full_version_with_label_from_release_tag "apache-polaris-1.2.3-incubating-beta"
  assert_output "1.2.3-incubating-beta"
  assert_success

  run full_version_with_label_from_release_tag "apache-polaris-1.2.3-incubating"
  assert_output "1.2.3-incubating"
  assert_success

  run full_version_with_label_from_release_tag "apache-polaris-1.2.3-beta"
  assert_output "1.2.3-beta"
  assert_success

  run full_version_with_label_from_release_tag "apache-polaris-1.2.3"
  assert_output "1.2.3"
  assert_success
}

# shellcheck disable=SC2034
@test "patch_version_from_rc_tag" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_releases_lib.sh"

  run patch_version_from_rc_tag "apache-polaris-1.2.3-incubating-beta-rc42"
  assert_output "3"
  assert_success

  run patch_version_from_rc_tag "apache-polaris-1.2.3-incubating-rc42"
  assert_output "3"
  assert_success

  run patch_version_from_rc_tag "apache-polaris-1.2.3-beta-rc42"
  assert_output "3"
  assert_success

  run patch_version_from_rc_tag "apache-polaris-1.2.3-rc42"
  assert_output "3"
  assert_success

  run patch_version_from_rc_tag "apache-polaris-1.2.3-incubating-beta"
  assert_output "" # not an RC tag
  assert_failure

  run patch_version_from_rc_tag "apache-polaris-1.2.3-incubating"
  assert_output "" # not an RC tag
  assert_failure

  run patch_version_from_rc_tag "apache-polaris-1.2.3-beta"
  assert_output "" # not an RC tag
  assert_failure

  run patch_version_from_rc_tag "apache-polaris-1.2.3"
  assert_output "" # not an RC tag
  assert_failure
}

# shellcheck disable=SC2034
@test "rc_iteration_from_tag" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_releases_lib.sh"

  run rc_iteration_from_tag "apache-polaris-1.2.3-incubating-beta-rc42"
  assert_output "42"
  assert_success

  run rc_iteration_from_tag "apache-polaris-1.2.3-incubating-rc42"
  assert_output "42"
  assert_success

  run rc_iteration_from_tag "apache-polaris-1.2.3-beta-rc42"
  assert_output "42"
  assert_success

  run rc_iteration_from_tag "apache-polaris-1.2.3-rc42"
  assert_output "42"
  assert_success

  run rc_iteration_from_tag "apache-polaris-1.2.3-incubating-beta"
  assert_output "" # not an RC tag
  assert_failure

  run rc_iteration_from_tag "apache-polaris-1.2.3-incubating"
  assert_output "" # not an RC tag
  assert_failure

  run rc_iteration_from_tag "apache-polaris-1.2.3-beta"
  assert_output "" # not an RC tag
  assert_failure

  run rc_iteration_from_tag "apache-polaris-1.2.3"
  assert_output "" # not an RC tag
  assert_failure
}

# shellcheck disable=SC2034
@test "version_label_from_rc_tag" {
  bin_dir="${RELEASE_BIN}"
  command_name="test_libs"

  . "$RELEASE_BIN/_releases_lib.sh"

  run version_label_from_rc_tag "apache-polaris-1.2.3-incubating-beta"
  assert_output "" # not an RC tag
  assert_failure

  run version_label_from_rc_tag "apache-polaris-1.2.3-incubating"
  assert_output "" # not an RC tag
  assert_failure

  run version_label_from_rc_tag "apache-polaris-1.2.3-beta"
  assert_output "" # not an RC tag
  assert_failure

  run version_label_from_rc_tag "apache-polaris-1.2.3"
  assert_output "" # not an RC tag
  assert_failure

  run version_label_from_rc_tag "apache-polaris-1.2.3-incubating-beta-rc42"
  assert_output "-incubating-beta"
  assert_success

  run version_label_from_rc_tag "apache-polaris-1.2.3-incubating-rc42"
  assert_output "-incubating"
  assert_success

  run version_label_from_rc_tag "apache-polaris-1.2.3-beta-rc42"
  assert_output "-beta"
  assert_success

  run version_label_from_rc_tag "apache-polaris-1.2.3-rc42"
  assert_output ""
  assert_success
}
