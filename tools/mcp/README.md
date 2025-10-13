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

| Variable                                                       | Description                                                    | Default                  |
|----------------------------------------------------------------|----------------------------------------------------------------|--------------------------|
| `POLARIS_BASE_URL`                                             | Base URL for all Polaris REST calls.                           | `http://localhost:8181/` |
| `POLARIS_API_TOKEN` / `POLARIS_BEARER_TOKEN` / `POLARIS_TOKEN` | Bearer token automatically attached to requests (if provided). | _unset_                  |

To authenticate via the built-in OAuth flow you can generate a token like this:
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

Config example for Claude desktop
```bash
vim ~/Library/Application\ Support/Claude/claude_desktop_config.json
```
Add the following changes to it:
```json
{
    "mcpServers": {
      "polaris": {
        "command": "java",
        "args": [
          "-cp",
          "/Users/ygu/tmp/polaris/tools/mcp/build/libs/polaris-mcp-1.2.0-incubating-SNAPSHOT.jar:/Users/ygu/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-databind/2.20.0/f0a5e62fbd21285e9a5498a60dccb097e1ef793b/jackson-databind-2.20.0.jar:/Users/ygu/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-core/2.20.0/3c97f7fad069f7cfae639d790bd93d6a0b2dff31/jackson-core-2.20.0.jar:/Users/ygu/.gradle/caches/modules-2/files-2.1/com.fasterxml.jackson.core/jackson-annotations/2.20/6a5e7291ea3f2b590a7ce400adb7b3aea4d7e12c/jackson-annotations-2.20.jar",
          "org.apache.polaris.tools.mcp.PolarisMcpServer"
         ],
        "env": {
          "POLARIS_BASE_URL": "http://localhost:8181/",
          "POLARIS_API_TOKEN": "<paste_access_token_here>" 
         }
      }
    }
}
```

The server currently exposes two MCP tools:

* `polaris-table-request` — High-level helper for the table REST API. Supported operations include:
  * `list`: `GET /api/catalog/v1/{catalog}/namespaces/{namespace}/tables`
  * `get` (aliases `load`, `fetch`): `GET /api/catalog/v1/{catalog}/namespaces/{namespace}/tables/{table}`
  * `create`: `POST /api/catalog/v1/{catalog}/namespaces/{namespace}/tables`
  * `commit` (alias `update`): `POST /api/catalog/v1/{catalog}/namespaces/{namespace}/tables/{table}`
  * `delete` (alias `drop`): `DELETE /api/catalog/v1/{catalog}/namespaces/{namespace}/tables/{table}`
* `polaris-policy-request` — Endpoints for policy lifecycle management. Supported operations include:
  * `list`: `GET /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies`
  * `get` (aliases `load`, `fetch`): `GET /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies/{policy}`
  * `create`: `POST /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies`
  * `update`: `PUT /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies/{policy}`
  * `delete` (aliases `drop`, `remove`): `DELETE /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies/{policy}`
  * `attach` / `detach`: `PUT`/`POST /api/catalog/polaris/v1/{catalog}/namespaces/{namespace}/policies/{policy}/mappings`
  * `applicable`: `GET /api/catalog/polaris/v1/{catalog}/applicable-policies`

Each operation returns the HTTP status, headers, and response body (pretty-printed
when JSON) to the MCP client, along with structured metadata that includes
request/response details.
