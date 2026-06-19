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

import os
import shlex
import sys
from dataclasses import dataclass
from typing import Optional
from cmd import Cmd

try:
    import readline
except ImportError:
    readline = None  # type: ignore[assignment]

from apache_polaris.cli.command import Command
from apache_polaris.cli.constants import (
    Commands,
    Arguments,
    REPL_HISTORY_LENGTH,
    REPL_HISTORY_FILE,
)
from apache_polaris.cli.exceptions import CliError
from apache_polaris.cli.options.option_tree import OptionTree
from apache_polaris.cli.options.parser import Parser
from apache_polaris.sdk.management import PolarisDefaultApi
from apache_polaris.sdk.management.exceptions import ApiException
from apache_polaris.cli.command.profiles import ProfilesCommand
from apache_polaris.cli.polaris_cli import PolarisCli
from urllib.parse import urlparse


@dataclass
class ReplCommand(Command):
    """
    A Command implementation to represent `polaris repl`. This command starts an interactive REPL session.

    Example commands:
        * polaris repl
    """

    profile: Optional[str] = None
    catalog: Optional[str] = None

    def validate(self) -> None:
        pass

    def execute(self, api: PolarisDefaultApi) -> None:
        repl = PolarisRepl(api, profile=self.profile, catalog=self.catalog)
        while True:
            try:
                repl.cmdloop()
                break
            except KeyboardInterrupt:
                sys.stdout.write("\n")
                repl.intro = ""


class PolarisRepl(Cmd):
    intro = "\n".join(
        [
            "Welcome to the Apache Polaris CLI REPL. Type 'help' for commands, 'exit' or Ctrl-D to quit.",
            "Note: global auth flags (--host, --port, --profile, --client-id, --client-secret, ...) are bound "
            "at session start and ignored when re-specified inside the REPL.",
        ]
    )

    def __init__(
        self,
        api: PolarisDefaultApi,
        profile: Optional[str] = None,
        catalog: Optional[str] = None,
    ):
        super().__init__()
        self.api = api
        self.catalog = catalog
        display_name = profile or urlparse(api.api_client.configuration.host).netloc
        if catalog:
            self.prompt = f"polaris@{display_name}/{catalog}> "
        else:
            self.prompt = f"polaris@{display_name}> "
        if readline is not None:
            try:
                readline.read_history_file(REPL_HISTORY_FILE)
            except (FileNotFoundError, OSError):
                pass
            readline.set_history_length(REPL_HISTORY_LENGTH)

    def default(self, line: str) -> None:
        if not line.strip():
            return
        try:
            args = shlex.split(line)
            options = Parser.parse(args)
            if options.command == Commands.REPL:
                sys.stderr.write("Already in REPL session.\n")
                return
            if (
                self.catalog
                and hasattr(options, Arguments.CATALOG)
                and getattr(options, Arguments.CATALOG) is None
            ):
                setattr(options, Arguments.CATALOG, self.catalog)
            command = Command.from_options(options)
            if isinstance(command, ProfilesCommand):
                command.execute()
            else:
                command.execute(self.api)
        except SystemExit:
            pass
        except KeyboardInterrupt:
            sys.stderr.write("Session interrupted. Type 'exit' to quit.\n")
        except ApiException as e:
            PolarisCli.print_api_exception(e)
        except CliError as e:
            sys.stderr.write(f"{e}\n")
        except NotImplementedError as e:
            sys.stderr.write(f"Internal error: {e}\n")
        except Exception as e:
            sys.stderr.write(f"An unexpected error occurred: {e}\n")

    def do_help(self, arg: str) -> None:
        if arg:
            self.default(f"{arg} --help")
            return
        sys.stdout.write(f"Polaris commands:{os.linesep}")
        for option in OptionTree.get_tree():
            sys.stdout.write(f" {option.name:<15} {option.hint or ''}\n")
        sys.stdout.write(
            f"{os.linesep}".join(
                [
                    "REPL built-ins: exit, help, Ctrl-D\n",
                    "Use 'help <command>' or '<command --help>' for command details.\n",
                ]
            )
        )

    def do_exit(self, args: str) -> bool:
        return True

    do_EOF = do_exit

    def emptyline(self) -> bool:
        return False

    def postloop(self) -> None:
        if readline is not None:
            try:
                os.makedirs(os.path.dirname(REPL_HISTORY_FILE), exist_ok=True)
                readline.write_history_file(REPL_HISTORY_FILE)
            except OSError:
                pass
