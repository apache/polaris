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

import subprocess
from pathlib import Path

SPARK_TGZ_URL = "https://www.apache.org/dyn/closer.lua/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz?action=download"


def ensure_spark(
    spark_sql_bin: Path, spark_dir: Path, spark_tarball_url: str, spark_download_requested: bool = False
) -> None:
    """
    Ensure that Spark is installed.

    :param spark_sql_bin: The `spark-sql` binary (must be within `spark_dir`).
    :param spark_dir: Location of the Spark installation.
    :param spark_tarball_url: The URL of the Spark tarball to download.
    :param spark_download_requested: Flag indicating whether a download should always be performed.
    """
    print("::group::Check for Spark installation")
    if not spark_download_requested and spark_sql_bin.exists():
        print(f"Spark binary {spark_sql_bin} is present")
        print("::endgroup::")
        return
    print(
        f"Spark binary {spark_sql_bin} is not present or download has been explicitly requested, "
        f"downloading from {spark_tarball_url}..."
    )
    curl_cmd = ["curl", "--location", spark_tarball_url]
    tar_cmd = [
        "tar",
        "--extract",
        "--gunzip",
        "--directory",
        str(spark_dir),
        "--strip-components=1",
    ]
    with subprocess.Popen(curl_cmd, stdout=subprocess.PIPE) as curl_proc:
        with subprocess.Popen(tar_cmd, stdin=curl_proc.stdout) as tar_proc:
            assert curl_proc.stdout is not None
            curl_proc.stdout.close()
            tar_rc = tar_proc.wait()
        curl_rc = curl_proc.wait()
        if curl_rc != 0:
            raise subprocess.CalledProcessError(curl_rc, curl_cmd)
        if tar_rc != 0:
            raise subprocess.CalledProcessError(tar_rc, tar_cmd)
    print("::endgroup::")
