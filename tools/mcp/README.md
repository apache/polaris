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

```bash
java -cp "$(./gradlew -q :polaris-mcp:printRuntimeClasspath)" \
  org.apache.polaris.tools.mcp.PolarisMcpServer
```

The server communicates over STDIN/STDOUT using JSON-RPC 2.0. Most MCP
clients expect to spawn the server as a subprocess with pipes connected to
standard input and output.

## Configuration

Configuration is supplied through environment variables (system properties
with the same names are also supported):

| Variable | Description | Default |
|----------|-------------|---------|
| `POLARIS_BASE_URL` | Base URL for all Polaris REST calls. | `http://localhost:8181/` |
| `POLARIS_API_TOKEN` / `POLARIS_BEARER_TOKEN` / `POLARIS_TOKEN` | Bearer token automatically attached to requests (if provided). | _unset_ |

```bash
export POLARIS_BASE_URL=http://localhost:8181/
```

The server currently exposes a single MCP tool:

* `polaris.table.request` — High-level helper for the table REST API. Supported operations include:
  * `list`: `GET /api/catalog/v1/{catalog}/namespaces/{namespace}/tables`
  * `get` (aliases `load`, `fetch`): `GET /api/catalog/v1/{catalog}/namespaces/{namespace}/tables/{table}`
  * `create`: `POST /api/catalog/v1/{catalog}/namespaces/{namespace}/tables`
  * `commit` (alias `update`): `POST /api/catalog/v1/{catalog}/namespaces/{namespace}/tables/{table}`
  * `delete` (alias `drop`): `DELETE /api/catalog/v1/{catalog}/namespaces/{namespace}/tables/{table}`

Each operation returns the HTTP status, headers, and response body (pretty-printed
when JSON) to the MCP client, along with structured metadata that includes
request/response details.
