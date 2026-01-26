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
from typing import Iterable, Optional


class Tee:
    def __init__(self, *files) -> None:
        self._files = files

    def write(self, data: str) -> None:
        for f in self._files:
            f.write(data)
            f.flush()

    def flush(self) -> None:
        for f in self._files:
            f.flush()

    def run(
        self,
        cmd: Iterable[str],
        cwd: Optional[Path] = None,
        env: Optional[dict[str, str]] = None,
        check: bool = False,
    ) -> int:
        proc = subprocess.Popen(
            list(cmd),
            cwd=str(cwd) if cwd else None,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            self.write(line)
        proc.stdout.close()
        rc = proc.wait()
        if check and rc != 0:
            raise subprocess.CalledProcessError(rc, list(cmd))
        return rc

    def printf(self, *args: object) -> None:
        print(*args, file=self, flush=True)
