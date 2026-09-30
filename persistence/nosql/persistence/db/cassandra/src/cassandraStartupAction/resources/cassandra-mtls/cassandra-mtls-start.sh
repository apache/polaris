#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail

cat /etc/cassandra/tls/client.key /etc/cassandra/tls/client.crt > /etc/cassandra/tls/server.pem

python3 - <<'PY'
from pathlib import Path

path = Path("/etc/cassandra/cassandra.yaml")
contents = path.read_text()
start = contents.index("client_encryption_options:")
end = contents.index("\n# internode_compression", start)
replacement = """client_encryption_options:
  enabled: true
  optional: false
  require_client_auth: true
  keystore: /etc/cassandra/tls/server.pem
  truststore: /etc/cassandra/tls/client.crt
  ssl_context_factory:
    class_name: org.apache.cassandra.security.PEMBasedSslContextFactory
"""
path.write_text(contents[:start] + replacement + contents[end:])
PY

exec docker-entrypoint.sh cassandra -f
