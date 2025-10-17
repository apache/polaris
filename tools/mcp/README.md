<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
   http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Polaris MCP Server

The `polaris-mcp` module provides a Java implementation of the
[Model Context Protocol (MCP)](https://modelcontextprotocol.io) that
wraps the Polaris REST APIs. It allows MCP-compatible clients (for
example, IDEs or chat-based agents) to call Polaris endpoints through
tools exposed by the server. The initial focus is on the Iceberg table
REST endpoints so that agents can list, inspect, modify, or delete
tables via structured MCP requests.

## Building

```bash
./gradlew :polaris-mcp:jar
```

Because this module depends on Jackson, the resulting JAR must be run with
its runtime dependencies on the classpath. You can obtain the runtime classpath
using the helper task:

```bash
./gradlew :polaris-mcp:printRuntimeClasspath
```

## Running

The server communicates over STDIN/STDOUT using JSON-RPC 2.0. Most MCP
clients expect to spawn the server as a subprocess with pipes connected to
standard input and output.

A typical launch sequence looks like:

```bash
./gradlew :polaris-mcp:classes
CP=$(./gradlew :polaris-mcp:printRuntimeClasspath --console=plain | grep '^/')

export POLARIS_BASE_URL="http://localhost:8181/"      # adjust as needed
export POLARIS_API_TOKEN="<bearer_token_optional>"

java -cp "$CP" org.apache.polaris.tools.mcp.PolarisMcpServer
```

Once started, the process waits for JSON-RPC messages on stdin. You can experiment manually, for example:

```jsonc
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"manual","version":"0"}}}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"polaris-table-request","arguments":{"operation":"list","catalog":"dev","namespace":"analytics.daily","query":{"page-size":"10"}}}}
```

Each `tools/call` response includes a human-readable block in
`result.content[0].text` and a structured JSON copy of the HTTP exchange in
`result.meta`. Errors surface as `result.isError = true`.

## Configuration

Configuration is supplied through environment variables (system properties
with the same names are also supported):

| Variable                                                       | Description                                              | Default                                          |
|----------------------------------------------------------------|----------------------------------------------------------|--------------------------------------------------|
| `POLARIS_BASE_URL`                                             | Base URL for all Polaris REST calls.                     | `http://localhost:8181/`                         |
| `POLARIS_API_TOKEN` / `POLARIS_BEARER_TOKEN` / `POLARIS_TOKEN` | Static bearer token (if supplied, overrides other auth). | _unset_                                          |
| `POLARIS_CLIENT_ID`                                            | OAuth client id for client-credential flow.              | _unset_                                          |
| `POLARIS_CLIENT_SECRET`                                        | OAuth client secret.                                     | _unset_                                          |
| `POLARIS_TOKEN_SCOPE`                                          | OAuth scope string.                                      | _unset_                                          |
| `POLARIS_TOKEN_URL`                                            | Optional override for the token endpoint URL.            | `${POLARIS_BASE_URL}api/catalog/v1/oauth/tokens` |

If `POLARIS_CLIENT_ID`, `POLARIS_CLIENT_SECRET`, `POLARIS_TOKEN_SCOPE` are set, the MCP server automatically requests and refreshes access tokens using the client credentials flow. If you prefer to manage tokens manually, you can generate one like this:

```bash
curl -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials' \
  -d 'client_id=root' \
  -d 'client_secret=s3cr3t' \
  -d 'scope=PRINCIPAL_ROLE:ALL'
```

The MCP server will then attach `Authorization: Bearer $POLARIS_API_TOKEN` to
every outgoing request.

## Config a MCP Client(Claude Desktop)
Open the config file:
```bash
vim ~/Library/Application\ Support/Claude/claude_desktop_config.json
```
Add the Polaris MCP to it:
```json
{
    "mcpServers": {
      "polaris": {
        "command": "java",
        "args": [
          "-cp",
          "<classpaths got from ./gradlew :polaris-mcp:printRuntimeClasspath>",
          "org.apache.polaris.tools.mcp.PolarisMcpServer"
         ],
        "env": {
          "POLARIS_BASE_URL": "http://localhost:8181/",
          "POLARIS_CLIENT_ID": "<client id>",
          "POLARIS_CLIENT_SECRET": "<client secret>",
          "POLARIS_TOKEN_SCOPE": "PRINCIPAL_ROLE:ALL"
         }
      }
    }
}
```

The server currently exposes seven MCP tools:

* `polaris-table-request` — High-level helper for the table REST API. Supported operations include:
  * `list`: `GET /api/catalog/v1/{catalog}/namespaces/{namespace}/tables`
  * `get` (aliases `load`, `fetch`): `GET /api/catalog/v1/{catalog}/namespaces/{namespace}/tables/{table}`
  * `create`: `POST /api/catalog/v1/{catalog}/namespaces/{namespace}/tables`
  * `commit` (alias `update`): `POST /api/catalog/v1/{catalog}/namespaces/{namespace}/tables/{table}`
  * `delete` (alias `drop`): `DELETE /api/catalog/v1/{catalog}/namespaces/{namespace}/tables/{table}`
* `polaris-namespace-request` — Namespace lifecycle helper:
  * `list`: `GET /api/catalog/v1/{catalog}/namespaces`
  * `get`: `GET /api/catalog/v1/{catalog}/namespaces/{namespace}`
  * `exists`: `HEAD /api/catalog/v1/{catalog}/namespaces/{namespace}`
  * `create`: `POST /api/catalog/v1/{catalog}/namespaces`
  * `update-properties`: `POST /api/catalog/v1/{catalog}/namespaces/{namespace}/properties`
  * `get-properties`: `GET /api/catalog/v1/{catalog}/namespaces/{namespace}/properties`
  * `delete`: `DELETE /api/catalog/v1/{catalog}/namespaces/{namespace}`
* `polaris-policy-request` — Endpoints for policy lifecycle management. Supported operations include:
  * `list`: `GET /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies`
  * `get` (aliases `load`, `fetch`): `GET /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies/{policy}`
  * `create`: `POST /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies`
  * `update`: `PUT /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies/{policy}`
  * `delete` (aliases `drop`, `remove`): `DELETE /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies/{policy}`
  * `attach` / `detach`: `PUT`/`POST /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies/{policy}/mappings`
  * `applicable`: `GET /api/catalog/polaris/v1/{catalog}/applicable-policies`
* `polaris-catalog-request` — Polaris Management API helper:
  * `list`: `GET /api/management/v1/catalogs`
  * `get`: `GET /api/management/v1/catalogs/{catalog}`
  * `create`: `POST /api/management/v1/catalogs`
  * `update`: `PUT /api/management/v1/catalogs/{catalog}`
  * `delete`: `DELETE /api/management/v1/catalogs/{catalog}`
* `polaris-principal-request` — Principal lifecycle helper:
  * `list`: `GET /api/management/v1/principals`
  * `create`: `POST /api/management/v1/principals`
  * `get`: `GET /api/management/v1/principals/{principal}`
  * `update`: `PUT /api/management/v1/principals/{principal}`
  * `delete`: `DELETE /api/management/v1/principals/{principal}`
  * `rotate-credentials`: `POST /api/management/v1/principals/{principal}/rotate`
  * `reset-credentials`: `POST /api/management/v1/principals/{principal}/reset`
  * `list-principal-roles`: `GET /api/management/v1/principals/{principal}/principal-roles`
  * `assign-principal-role`: `PUT /api/management/v1/principals/{principal}/principal-roles`
  * `revoke-principal-role`: `DELETE /api/management/v1/principals/{principal}/principal-roles/{principalRole}`
* `polaris-principal-role-request` — Principal role and mapping helper:
  * `list`: `GET /api/management/v1/principal-roles`
  * `create`: `POST /api/management/v1/principal-roles`
  * `get`: `GET /api/management/v1/principal-roles/{principalRole}`
  * `update`: `PUT /api/management/v1/principal-roles/{principalRole}`
  * `delete`: `DELETE /api/management/v1/principal-roles/{principalRole}`
  * `list-principals`: `GET /api/management/v1/principal-roles/{principalRole}/principals`
  * `list-catalog-roles`: `GET /api/management/v1/principal-roles/{principalRole}/catalog-roles/{catalog}`
  * `assign-catalog-role`: `PUT /api/management/v1/principal-roles/{principalRole}/catalog-roles/{catalog}`
  * `revoke-catalog-role`: `DELETE /api/management/v1/principal-roles/{principalRole}/catalog-roles/{catalog}/{catalogRole}`
* `polaris-catalog-role-request` — Catalog role and grants helper:
  * `list`: `GET /api/management/v1/catalogs/{catalog}/catalog-roles`
  * `create`: `POST /api/management/v1/catalogs/{catalog}/catalog-roles`
  * `get`: `GET /api/management/v1/catalogs/{catalog}/catalog-roles/{catalogRole}`
  * `update`: `PUT /api/management/v1/catalogs/{catalog}/catalog-roles/{catalogRole}`
  * `delete`: `DELETE /api/management/v1/catalogs/{catalog}/catalog-roles/{catalogRole}`
  * `list-principal-roles`: `GET /api/management/v1/catalogs/{catalog}/catalog-roles/{catalogRole}/principal-roles`
  * `list-grants`: `GET /api/management/v1/catalogs/{catalog}/catalog-roles/{catalogRole}/grants`
  * `add-grant`: `PUT /api/management/v1/catalogs/{catalog}/catalog-roles/{catalogRole}/grants`
  * `revoke-grant`: `POST /api/management/v1/catalogs/{catalog}/catalog-roles/{catalogRole}/grants`

Each operation returns the HTTP status, headers, and response body (pretty-printed
when JSON) to the MCP client, along with structured metadata that includes
request/response details.
