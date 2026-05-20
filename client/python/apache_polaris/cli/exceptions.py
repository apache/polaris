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

"""Typed exceptions for the Polaris CLI (user-facing errors vs unexpected bugs)."""

# Exit code for argparse errors and typical usage mistakes (matches POSIX EX_USAGE).
CLI_USAGE_EXIT_CODE = 2

# Exit code for runtime failures: Polaris API errors, failed token exchange, unexpected bugs.
# ``polaris_cli`` uses this same value for ``ApiException`` and generic ``Exception`` exits.
CLI_ERROR_EXIT_CODE = 1


class CliError(Exception):
    """
    Expected CLI failure with a message suitable for stderr.

    Use exit_code ``CLI_USAGE_EXIT_CODE`` for invalid arguments and validation;
    use ``CLI_ERROR_EXIT_CODE`` for failures after contacting the server or for
    internal preconditions that are not argparse validation.
    """

    __slots__ = ("exit_code",)

    def __init__(self, message: str, *, exit_code: int = CLI_USAGE_EXIT_CODE) -> None:
        super().__init__(message)
        self.exit_code = exit_code
