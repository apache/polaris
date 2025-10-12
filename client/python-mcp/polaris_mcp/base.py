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

"""Shared protocol definitions for the Polaris MCP server."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol


JSONDict = Dict[str, Any]


@dataclass(frozen=True)
class ToolExecutionResult:
    """Structured result returned from executing an MCP tool."""

    text: str
    is_error: bool
    metadata: Optional[JSONDict] = None


class McpTool(Protocol):
    """Protocol describing the minimal surface for MCP tools."""

    @property
    def name(self) -> str:  # pragma: no cover - simple accessor
        ...

    @property
    def description(self) -> str:  # pragma: no cover - simple accessor
        ...

    def input_schema(self) -> JSONDict:
        """Return a JSON schema describing the tool parameters."""

    def call(self, arguments: Any) -> ToolExecutionResult:
        """Execute the tool with the provided JSON arguments."""
