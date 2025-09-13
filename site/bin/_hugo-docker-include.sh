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

[[ -f site/.user-settings ]] && . site/.user-settings

[[ -z ${DOCKER} ]] && DOCKER="$(which podman > /dev/null && echo podman || echo docker)"
[[ -z ${COMPOSE} ]] && COMPOSE="$(which podman-compose > /dev/null && echo podman-compose || echo docker-compose)"

# absolute path to the Polaris workspace, for the volume mount
WORKSPACE="$(pwd)"
export WORKSPACE

# Mount options for the Polaris workspace / slight difference between Podman + docker / Linux + macOS.
# The Linux specialties are there to ensure that neither Docker nor Podman "mess up" file/directory ownership
# to for example root or an ephemeral UID/GID (rootless). Git for example gets quite annoyed when the owner
# of the worktree changes and then refuses to consider it as a valid Git worktree (aka git commands don't work).
MOUNT_OPTS_WORKSPACE="bind"
UID_GID=""
MOUNT_OPTS_RESOURCES=""
COMPOSE_ARGS=""
case "$(uname)" in
  Linux)
    # Don't do the Linux specific UID/GID dance in CI, it uses "low" UID/GID, not "real user" ones (>= 1000).
    # And there's no human who cares about broken ownership on the workspace directories/files in CI.
    if [[ -z ${CI} ]] ; then
      # Mount options
      MOUNT_OPTS_RESOURCES="uid=$(id -u),gid=$(id -g)"

      # Needed for podman on Linux, so it doesn't "chown" the Polaris workspace to an ephemeral UID (Linux only)
      MOUNT_OPTS_WORKSPACE="bind,$MOUNT_OPTS_RESOURCES"

      # uid/gid option for the container
      UID_GID="$(id -u):$(id -g)"

      if [[ ${COMPOSE} == "podman-compose" ]]; then
        # Let podman use the user's UID+GID in the running container
        COMPOSE_ARGS="--podman-run-args=--userns=keep-id"
      fi
    fi
    ;;
esac
export MOUNT_OPTS_WORKSPACE MOUNT_OPTS_RESOURCES UID_GID

mkdir -p site/build/hugo-cache

# Cleanup before start and on exist / just to be on the "safe side"
function cleanup() {
  $COMPOSE \
    --project-name polaris_site \
    --file site/docker/docker-compose-publish.yml \
    down > /dev/null 2>&1 || true
  $COMPOSE \
    --project-name polaris_site \
    --file site/docker/docker-compose-hugo.yml \
    down > /dev/null 2>&1 || true
  $DOCKER container rm -f polaris_site-hugo-1 polaris_site-hugo_publish_localhost8080-1 polaris_site-hugo_publish_apache-1 > /dev/null 2>&1 || true
  $DOCKER volume rm -f polaris_site_resources polaris_site_workspace polaris_site_hugo_cache > /dev/null 2>&1 || true
}

cleanup

trap 'cleanup' EXIT
