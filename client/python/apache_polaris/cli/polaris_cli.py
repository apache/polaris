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
import functools
import json
import os
import sys
from json import JSONDecodeError

import urllib3

from apache_polaris.cli.api_client_builder import ApiClientBuilder
from apache_polaris.cli.constants import Commands
from apache_polaris.cli.options.parser import Parser
from apache_polaris.sdk.management import PolarisDefaultApi


class PolarisCli:
    """
    Implements a basic Command-Line Interface (CLI) for interacting with a Polaris service. The CLI can be used to
    manage entities like catalogs, principals, and grants within Polaris and can perform most operations that are
    available in the Python client API.

    Example usage:
    * ./polaris --client-id ${id} --client-secret ${secret} --host ${hostname} --port ${port} principals create example_user
    * ./polaris --client-id ${id} --client-secret ${secret} --host ${hostname} --port ${port} principal-roles create example_role
    * ./polaris --client-id ${id} --client-secret ${secret} --host ${hostname} --port ${port} catalog-roles list
    * ./polaris --client-id ${id} --client-secret ${secret} --base-url https://custom-polaris-domain.example.com/service-prefix catalogs list
    """

    # Can be enabled if the client is able to authenticate directly without first fetching a token
    DIRECT_AUTHENTICATION_ENABLED = False

    @staticmethod
    def _patch_generated_models() -> None:
        """
        The OpenAPI generator creates an `api_client` that dynamically looks up
        model classes from the `apache_polaris.sdk.catalog.models` module using `getattr()`.
        For example, when a response for a `create_policy` call is received, the
        deserializer tries to find the `LoadPolicyResponse` class by looking for
        `apache_polaris.sdk.catalog.models.LoadPolicyResponse`.

        However, the generator fails to add the necessary `import` statements
        to the `polaris/catalog/models/__init__.py` file. This means that even
        though the model files exist (e.g., `load_policy_response.py`), the classes
        are not part of the `apache_polaris.sdk.catalog.models` namespace.

        This method works around the bug in the generated code without modifying
        the source files. It runs once per CLI execution, before any commands, and
        manually injects the missing response-side model classes into the
        `apache_polaris.sdk.catalog.models` namespace, allowing the deserializer to find them.
        """
        import apache_polaris.sdk.catalog.models
        from apache_polaris.sdk.catalog.models.applicable_policy import ApplicablePolicy
        from apache_polaris.sdk.catalog.models.get_applicable_policies_response import GetApplicablePoliciesResponse
        from apache_polaris.sdk.catalog.models.list_policies_response import ListPoliciesResponse
        from apache_polaris.sdk.catalog.models.load_policy_response import LoadPolicyResponse
        from apache_polaris.sdk.catalog.models.policy import Policy
        from apache_polaris.sdk.catalog.models.policy_attachment_target import PolicyAttachmentTarget
        from apache_polaris.sdk.catalog.models.policy_identifier import PolicyIdentifier

        models_to_patch = {
            "ApplicablePolicy": ApplicablePolicy,
            "GetApplicablePoliciesResponse": GetApplicablePoliciesResponse,
            "ListPoliciesResponse": ListPoliciesResponse,
            "LoadPolicyResponse": LoadPolicyResponse,
            "Policy": Policy,
            "PolicyAttachmentTarget": PolicyAttachmentTarget,
            "PolicyIdentifier": PolicyIdentifier,
        }

        for name, model_class in models_to_patch.items():
            setattr(apache_polaris.sdk.catalog.models, name, model_class)

    @staticmethod
    def execute(args=None):
        PolarisCli._patch_generated_models()
        options = Parser.parse(args)
        if options.command == Commands.PROFILES:
            from apache_polaris.cli.command import Command

            command = Command.from_options(options)
            command.execute()
        else:
            api_client = ApiClientBuilder(
                options, direct_authentication=PolarisCli.DIRECT_AUTHENTICATION_ENABLED
            ).get_api_client()
            try:
                from apache_polaris.cli.command import Command

                admin_api = PolarisDefaultApi(api_client)
                command = Command.from_options(options)
                if options.debug:
                    PolarisCli._enable_api_request_logging()
                command.execute(admin_api)
            except Exception as e:
                PolarisCli._try_print_exception(e)
                sys.exit(1)

    @staticmethod
    def _enable_api_request_logging():
        # Store the original urlopen method
        if not hasattr(urllib3.PoolManager, "original_urlopen"):
            urllib3.PoolManager.original_urlopen = urllib3.PoolManager.urlopen

        # Define the wrapper function
        @functools.wraps(urllib3.PoolManager.original_urlopen)
        def urlopen_wrapper(self, method, url, **kwargs):
            sys.stderr.write(f"Request: {method} {url}\n")
            if "headers" in kwargs:
                sys.stderr.write(f"Headers: {kwargs['headers']}\n")
            if "body" in kwargs:
                sys.stderr.write(f"Body: {kwargs['body']}\n")
            sys.stderr.write("\n")
            # Call the original urlopen method
            return urllib3.PoolManager.original_urlopen(self, method, url, **kwargs)

        urllib3.PoolManager.urlopen = urlopen_wrapper

    @staticmethod
    def _try_print_exception(e):
        try:
            error = json.loads(e.body)["error"]
            sys.stderr.write(
                f"Exception when communicating with the Polaris server."
                f" {error['type']}: {error['message']}{os.linesep}"
            )
        except JSONDecodeError as _:
            sys.stderr.write(
                f"Exception when communicating with the Polaris server."
                f" {e.status}: {e.reason}{os.linesep}"
            )
        except Exception as _:
            sys.stderr.write(
                f"Exception when communicating with the Polaris server. {e}{os.linesep}"
            )


def main():
    PolarisCli.execute()


if __name__ == "__main__":
    main()
