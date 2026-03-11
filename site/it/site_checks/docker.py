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

import json
import subprocess
import time

from .tee import Tee

DASH_LINE = "-" * 96


def cleanup_docker(tee: Tee) -> float:
    """
    Clean up (remove) Docker containers, networks, and volumes.

    :param tee: Log output.
    :return: Elapsed time (in seconds) spent running this cleanup.
    """
    start = time.monotonic()
    try:
        tee.printf("::group::Cleanup Docker containers, networks, volumes")
        tee.printf("Purging Docker containers...")
        result = subprocess.run(
            ["docker", "ps", "-a", "-q"],
            capture_output=True,
            text=True,
            check=False,
        )
        container_ids = [line for line in result.stdout.splitlines() if line.strip()]
        if container_ids:
            tee.run(["docker", "container", "rm", "-f", *container_ids])
        tee.printf("Purging Docker networks...")
        tee.run(["docker", "network", "prune", "-f"])
        tee.printf("Purging Docker volumes...")
        tee.run(["docker", "volume", "prune", "-f"])
    except Exception as e:
        tee.printf(f"Failed to cleanup Docker containers, networks, volumes: {repr(e)}")
        pass
    tee.printf("::endgroup::")
    return time.monotonic() - start


def docker_compose_info(tee: Tee, md_base_name: str, md_dir_name: str) -> float:
    """
    Emit docker compose status/logs for any known compose projects.

    :param tee: Log output.
    :param md_base_name: Markdown file base name (for logging/group naming).
    :param md_dir_name: Markdown file directory name (for logging/group naming).
    :return: Elapsed time (in seconds) spent collecting docker compose information.
    """
    start = time.monotonic()
    tee.printf(f"::group::Docker Compose information for {md_base_name} in {md_dir_name}")
    tee.printf(DASH_LINE)
    try:
        result = subprocess.run(
            ["docker", "compose", "ls", "--all", "--format", "json"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        tee.printf(DASH_LINE)
        tee.printf("::endgroup::")
        return time.monotonic() - start

    try:
        compose_entries = json.loads(result.stdout) if result.stdout.strip() else []
    except json.JSONDecodeError:
        compose_entries = []

    for entry in compose_entries:
        docker_compose_file = entry.get("ConfigFiles")
        if not docker_compose_file:
            continue
        tee.printf(DASH_LINE)
        tee.printf(f"Docker compose file {docker_compose_file}")
        tee.printf("    docker compose ps:")
        tee.run(
            ["docker", "compose", "-f", docker_compose_file, "ps", "--all", "--no-trunc", "--orphans"],
        )
        tee.printf("    docker compose logs:")
        tee.run(
            ["docker", "compose", "-f", docker_compose_file, "logs", "--timestamps"],
        )
    tee.printf(DASH_LINE)
    tee.printf("::endgroup::")
    return time.monotonic() - start
