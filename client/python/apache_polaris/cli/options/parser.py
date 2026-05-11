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
import argparse
from typing import List, Optional, Dict, Any, Union

from apache_polaris.cli.constants import Arguments, DEFAULT_HEADER
from apache_polaris.cli.exceptions import CliError
from apache_polaris.cli.options.option_tree import OptionTree, Option, Argument


class PolarisHelpFormatter(argparse.HelpFormatter):
    def __init__(self, prog: str) -> None:
        super().__init__(prog, max_help_position=100)


class Parser(object):
    """
    `Parser.parse()` is used to parse CLI input into an argparse.Namespace. The arguments expected by the parser are
    defined by `OptionTree.getTree()` and by the arguments in `Parser._ROOT_ARGUMENTS`. This class is responsible for
    translating the option tree into an ArgumentParser, for applying that ArgumentParser to the user input.
    """

    _ROOT_ARGUMENTS = [
        Argument(Arguments.HOST, str, hint="Polaris server hostname"),
        Argument(Arguments.PORT, int, hint="Polaris server port"),
        Argument(
            Arguments.BASE_URL, str, hint="Complete base URL (overrides host/port)"
        ),
        Argument(Arguments.CLIENT_ID, str, hint="OAuth client ID"),
        Argument(
            Arguments.CLIENT_SECRET,
            str,
            hint="OAuth client secret",
        ),
        Argument(
            Arguments.ACCESS_TOKEN,
            str,
            hint="OAuth access token",
        ),
        Argument(
            Arguments.REALM,
            str,
            hint="Polaris realm (default: from server)",
            default=None,
        ),
        Argument(
            Arguments.HEADER,
            str,
            hint="Context header name (default: Polaris-Realm)",
            default=DEFAULT_HEADER,
        ),
        Argument(Arguments.PROFILE, str, hint="Polaris profile name"),
        Argument(Arguments.PROXY, str, hint="Proxy URL"),
        Argument(Arguments.DEBUG, bool, hint="Enable debug mode"),
    ]

    @staticmethod
    def _add_arguments(
        parser: argparse.ArgumentParser,
        args: List[Argument],
        group_name: Optional[str] = None,
        is_subparser: bool = False,
        hide_help: bool = False,
    ) -> None:
        # Group arguments by their metadata group or the provided default group_name
        groups: Dict[Optional[str], List[Argument]] = {}
        for arg in args:
            target_group = arg.group if arg.group else group_name
            groups.setdefault(target_group, []).append(arg)

        for name, group_args in groups.items():
            # If we are hiding help for these (inherited globals in subparsers),
            # we add them directly to the parser suppressed.
            container: Union[argparse.ArgumentParser, argparse._ArgumentGroup]
            if hide_help:
                container = parser
            elif name:
                container = parser.add_argument_group(name)
            else:
                container = parser
            # Attach arguments for each of the group
            for arg in group_args:
                kwargs: Dict[str, Any] = {
                    "help": argparse.SUPPRESS if hide_help else arg.hint,
                    "type": arg.type,
                }
                if arg.choices:
                    kwargs["choices"] = arg.choices
                if arg.lower:
                    kwargs["type"] = lambda s: s.lower()

                # If this is a subparser, we suppress defaults to avoid overwriting
                # values already set in the namespace by parent parsers.
                if is_subparser:
                    kwargs["default"] = argparse.SUPPRESS
                elif arg.default is not None:
                    kwargs["default"] = arg.default

                if arg.metavar:
                    kwargs["metavar"] = arg.metavar

                if arg.type is bool:
                    del kwargs["type"]
                    if is_subparser:
                        kwargs["default"] = argparse.SUPPRESS
                    container.add_argument(
                        arg.get_flag_name(), **kwargs, action="store_true"
                    )
                elif arg.allow_repeats:
                    container.add_argument(
                        arg.get_flag_name(), **kwargs, action="append"
                    )
                else:
                    container.add_argument(arg.get_flag_name(), **kwargs)

    @staticmethod
    def build_parser() -> argparse.ArgumentParser:
        # Base parser for global arguments used by SUBPARSERS.
        # We suppress help for these to keep subcommand help clean.
        hidden_base_parser = argparse.ArgumentParser(add_help=False)
        Parser._add_arguments(
            hidden_base_parser,
            Parser._ROOT_ARGUMENTS,
            is_subparser=True,
            hide_help=True,
        )

        # Root parser
        parser = argparse.ArgumentParser(
            prog="polaris",
            usage="polaris [-h] [options] COMMAND ...",
            formatter_class=PolarisHelpFormatter,
        )
        Parser._add_arguments(parser, Parser._ROOT_ARGUMENTS, "Global Options")

        def recurse_options(
            subparser_container: Any, options: List[Option], parent_path: str
        ) -> None:
            for option in options:
                # Calculate the new path for this subcommand
                current_path = f"{parent_path} {option.name}"

                # Build usage parts
                usage_parts = [current_path, "[-h]", "[options]"]
                if option.children:
                    usage_parts.append("SUBCOMMAND ...")
                if option.input_name:
                    usage_parts.append(
                        option.input_metavar or option.input_name.upper()
                    )

                option_parser = subparser_container.add_parser(
                    option.name,
                    help=option.hint,
                    parents=[hidden_base_parser],
                    prog=current_path,
                    usage=" ".join(usage_parts),
                    formatter_class=PolarisHelpFormatter,
                )

                if option.args:
                    Parser._add_arguments(option_parser, option.args, "Command Options")

                if option.input_name:
                    option_parser.add_argument(
                        option.input_name,
                        type=str,
                        help=option.input_name.replace("_", " "),
                        metavar=option.input_metavar or option.input_name.upper(),
                        default=None,
                    )
                if option.children:
                    child_subparser_container = option_parser.add_subparsers(
                        title="Subcommands",
                        dest=f"{option.name}_subcommand",
                        required=True,
                        metavar="SUBCOMMAND",
                    )
                    recurse_options(
                        child_subparser_container, option.children, current_path
                    )

        subparser_container = parser.add_subparsers(
            title="Commands", dest="command", required=True, metavar="COMMAND"
        )
        recurse_options(subparser_container, OptionTree.get_tree(), "polaris")
        return parser

    @staticmethod
    def parse(args: Optional[List[str]] = None) -> argparse.Namespace:
        parser = Parser.build_parser()
        return parser.parse_args(args)

    @staticmethod
    def parse_properties(properties: List[str]) -> Optional[Dict[str, str]]:
        if not properties:
            return None
        results = dict()
        for property in properties:
            if "=" not in property:
                raise CliError(f"Could not parse property `{property}`")
            key, value = property.split("=", 1)
            if not value:
                raise CliError(f"Could not parse property `{property}`")
            if key in results:
                raise CliError(f"Duplicate property key `{key}`")
            results[key] = value
        return results
