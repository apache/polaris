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
import sys
from typing import List, Optional, Dict

from cli.constants import Arguments
from cli.options.option_tree import OptionTree, Option, Argument


class Parser(object):
    """
    `Parser.parse()` is used to parse CLI input into an argparse.Namespace. The arguments expected by the parser are
    defined by `OptionTree.getTree()` and by the arguments in `Parser._ROOT_ARGUMENTS`. This class is responsible for
    translating the option tree into an ArgumentParser, for applying that ArgumentParser to the user input, and for
    generating a custom help message based on the option tree.
    """

    """
    Generates an argparse parser based on the option tree.
    """

    _ROOT_ARGUMENTS = [
        Argument(Arguments.HOST, str, hint="hostname"),
        Argument(Arguments.PORT, int, hint="port"),
        Argument(
            Arguments.BASE_URL, str, hint="complete base URL instead of hostname:port"
        ),
        Argument(
            Arguments.CLIENT_ID, str, hint="client ID for token-based authentication"
        ),
        Argument(
            Arguments.CLIENT_SECRET,
            str,
            hint="client secret for token-based authentication",
        ),
        Argument(
            Arguments.ACCESS_TOKEN,
            str,
            hint="access token for token-based authentication",
        ),
        Argument(Arguments.PROFILE, str, hint="profile for token-based authentication"),
        Argument(Arguments.PROXY, str, hint="proxy URL"),
    ]

    @staticmethod
    def _build_parser() -> argparse.ArgumentParser:
        parser = TreeHelpParser(description="Polaris CLI")

        for arg in Parser._ROOT_ARGUMENTS:
            if arg.default is not None:
                parser.add_argument(
                    arg.get_flag_name(),
                    type=arg.type,
                    help=arg.hint,
                    default=arg.default,
                )
            else:
                parser.add_argument(arg.get_flag_name(), type=arg.type, help=arg.hint)

        # Add everything from the option tree to the parser:
        def add_arguments(parser, args: List[Argument]):
            for arg in args:
                kwargs = {"help": arg.hint, "type": arg.type}
                if arg.choices:
                    kwargs["choices"] = arg.choices
                if arg.lower:
                    kwargs["type"] = kwargs["type"].lower
                if arg.default:
                    kwargs["default"] = arg.default

                if arg.type is bool:
                    del kwargs['type']
                    parser.add_argument(arg.get_flag_name(), **kwargs, action='store_true')
                elif arg.allow_repeats:
                    parser.add_argument(arg.get_flag_name(), **kwargs, action="append")
                else:
                    parser.add_argument(arg.get_flag_name(), **kwargs)

        def recurse_options(subparser, options: List[Option]):
            for option in options:
                option_parser = subparser.add_parser(
                    option.name, help=option.hint or option.name
                )
                add_arguments(option_parser, option.args)
                if option.input_name:
                    option_parser.add_argument(
                        option.input_name,
                        type=str,
                        help=option.input_name.replace("_", " "),
                        default=None,
                    )
                if option.children:
                    children_subparser = option_parser.add_subparsers(
                        dest=f"{option.name}_subcommand", required=False
                    )
                    recurse_options(children_subparser, option.children)

        subparser = parser.add_subparsers(dest="command", required=False)
        recurse_options(subparser, OptionTree.get_tree())
        return parser

    @staticmethod
    def parse(input: Optional[List[str]] = None) -> argparse.Namespace:
        parser = Parser._build_parser()
        return parser.parse_args(input)

    @staticmethod
    def parse_properties(properties: List[str]) -> Optional[Dict[str, str]]:
        if not properties:
            return None
        results = dict()
        for property in properties:
            if "=" not in property:
                raise Exception(f"Could not parse property `{property}`")
            key, value = property.split("=", 1)
            if not value:
                raise Exception(f"Could not parse property `{property}`")
            if key in results:
                raise Exception(f"Duplicate property key `{key}`")
            results[key] = value
        return results


class TreeHelpParser(argparse.ArgumentParser):
    """
    Replaces the default help behavior with a more readable message.
    """

    INDENT = " " * 2

    def parse_args(self, args=None, namespace=None):
        if args is None:
            args = sys.argv[1:]
        help_index = min(
            [float("inf")] + [args.index(x) for x in ["-h", "--help"] if x in args]
        )
        if help_index < float("inf"):
            tree_str = self._get_tree_str(args[:help_index])
            if tree_str:
                print(f'input: polaris {" ".join(args)}')
                print('options:')
                print(tree_str)
                print("\n")
                self.print_usage()
                super().exit()
            else:
                return super().parse_args(args, namespace)
        else:
            return super().parse_args(args, namespace)

    def _get_tree_str(self, args: List[str]) -> Optional[str]:
        command_path = self._get_command_path(args, OptionTree.get_tree())
        if len(command_path) == 0:
            result = TreeHelpParser.INDENT + "polaris"
            for arg in Parser._ROOT_ARGUMENTS:
                result += (
                    "\n"
                    + (TreeHelpParser.INDENT * 2)
                    + f"{arg.get_flag_name()}  {arg.hint}"
                )
            for option in OptionTree.get_tree():
                result += "\n" + self._get_tree_for_option(option, indent=2)
            return result
        else:
            option_node = self._get_option_node(command_path, OptionTree.get_tree())
            if option_node is None:
                return None
            else:
                return self._get_tree_for_option(option_node)

    def _get_tree_for_option(self, option: Option, indent=1) -> str:
        result = ""
        result += (TreeHelpParser.INDENT * indent) + option.name

        if option.args:
            result += "\n" + (TreeHelpParser.INDENT * (indent + 1)) + "Named arguments:"
        for arg in option.args:
            result += (
                "\n"
                + (TreeHelpParser.INDENT * (indent + 2))
                + f"{arg.get_flag_name()}  {arg.hint}"
            )

        if option.input_name:
            result += (
                "\n" + (TreeHelpParser.INDENT * (indent + 1)) + "Positional arguments:"
            )
            result += "\n" + (TreeHelpParser.INDENT * (indent + 2)) + option.input_name

        if len(option.args) > 0 and len(option.children) > 0:
            result += "\n"

        for child in sorted(option.children, key=lambda o: o.name):
            result += "\n" + self._get_tree_for_option(child, indent + 1)

        return result

    def _get_command_path(self, args: List[str], options: List[Option]) -> List[str]:
        command_path = []
        parser = self

        while args:
            arg = args.pop(0)
            if arg in {o.name for o in options}:
                command_path.append(arg)
                try:
                    parser = parser._subparsers._group_actions[0].choices.get(arg)
                    if not parser:
                        break
                except Exception:
                    break
                options = list(filter(lambda o: o.name == arg, options))[0].children
                if options is None:
                    break
        return command_path

    def _get_option_node(
        self, command_path: List[str], nodes: List[Option]
    ) -> Optional[Option]:
        if len(command_path) > 0:
            for node in nodes:
                if node.name == command_path[0]:
                    if len(command_path) == 1:
                        return node
                    else:
                        return self._get_option_node(command_path[1:], node.children)
        return None
