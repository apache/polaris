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

# Apache Polaris MCP Server (Python)

This package provides a Python implementation of the [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server for Apache Polaris. It wraps the Polaris REST APIs so MCP-compatible clients (IDEs, agents, chat applications) can issue structured requests via JSON-RPC on stdin/stdout.

The implementation is built on top of [FastMCP](https://gofastmcp.com) for streamlined server registration and transport handling.

## Installation

From the repository root:

```bash
cd client/python-mcp
uv sync
```

## Running

Launch the MCP server (which reads from stdin and writes to stdout):

```bash
uv run polaris-mcp
```

Example interaction:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientInfo":{"name":"manual","version":"0"}}}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
```

For a `tools/call` invocation you will typically set environment variables such as `POLARIS_BASE_URL` and authentication settings before launching the server.

### Claude Desktop configuration

```json
{
  "mcpServers": {
    "polaris": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/polaris/client/python-mcp",
        "run",
        "polaris-mcp"
      ],
      "env": {
        "POLARIS_BASE_URL": "http://localhost:8181/",
        "POLARIS_CLIENT_ID": "root",
        "POLARIS_CLIENT_SECRET": "s3cr3t",
        "POLARIS_TOKEN_SCOPE": "PRINCIPAL_ROLE:ALL"
      }
    }
  }
}
```

Please note: `--directory` specifies a local directory. It is not needed when we pull `polaris-mcp` from PyPI package.

## Configuration

| Variable                                                       | Description                                              | Default                                          |
|----------------------------------------------------------------|----------------------------------------------------------|--------------------------------------------------|
| `POLARIS_BASE_URL`                                             | Base URL for all Polaris REST calls.                     | `http://localhost:8181/`                         |
| `POLARIS_API_TOKEN` / `POLARIS_BEARER_TOKEN` / `POLARIS_TOKEN` | Static bearer token (if supplied, overrides other auth). | _unset_                                          |
| `POLARIS_CLIENT_ID`                                            | OAuth client id for client-credential flow.              | _unset_                                          |
| `POLARIS_CLIENT_SECRET`                                        | OAuth client secret.                                     | _unset_                                          |
| `POLARIS_TOKEN_SCOPE`                                          | OAuth scope string.                                      | _unset_                                          |
| `POLARIS_TOKEN_URL`                                            | Optional override for the token endpoint URL.            | `${POLARIS_BASE_URL}api/catalog/v1/oauth/tokens` |

When OAuth variables are supplied, the server automatically acquires and refreshes tokens using the client credentials flow; otherwise a static bearer token is used if provided.

## Tools

The server exposes the following MCP tools:

* `polaris-iceberg-table` — Table operations (`list`, `get`, `create`, `update`, `delete`).
* `polaris-namespace-request` — Namespace lifecycle management.
* `polaris-policy` — Policy lifecycle management and mappings.
* `polaris-catalog-request` — Catalog lifecycle management.
* `polaris-principal-request` — Principal lifecycle helpers.
* `polaris-principal-role-request` — Principal role lifecycle and catalog-role assignments.
* `polaris-catalog-role-request` — Catalog role and grant management.

Each tool returns both a human-readable transcript of the HTTP exchange and structured metadata under `result.meta`.
