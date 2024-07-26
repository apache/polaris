from cli.options.parser import Parser
from polaris.management import ApiClient, Configuration, ApiException
from polaris.management import PolarisDefaultApi


class PolarisCli:
    """
    Implements a basic Command-Line Interface (CLI) for interacting with a Polaris service. The CLI can be used to
    manage entities like catalogs, principals, and grants within Polaris and can perform most operations that are
    available in the Python client API.

    Example usage:
    * ./polaris --client-id ${id} --client-secret ${secret} --host ${hostname} principals create example_user
    * ./polaris --client-id ${id} --client-secret ${secret} --host ${hostname} principal-roles create example_role
    * ./polaris --client-id ${id} --client-secret ${secret} --host ${hostname} catalog-roles list
    """

    @staticmethod
    def execute():
        options = Parser.parse()
        client_builder = PolarisCli._get_client_builder(options)
        with client_builder() as api_client:
            try:
                from cli.command import Command
                admin_api = PolarisDefaultApi(api_client)
                command = Command.from_options(options)
                command.execute(admin_api)
            except ApiException as e:
                import json
                error = json.loads(e.body)['error']
                print(f'Exception when communicating with the Polaris server. {error["type"]}: {error["message"]}')

    @staticmethod
    def _get_client_builder(options):

        # Validate
        has_access_token = options.access_token is not None
        has_client_secret = options.client_id is not None and options.client_secret is not None
        if has_access_token and has_client_secret:
            raise Exception("Please provide credentials via either --client-id / --client-secret or "
                            "--access-token, but not both")

        # Authenticate accordingly
        polaris_catalog_url = f'http://{options.host}:{options.port}/api/management/v1'
        if has_access_token:
            return lambda: ApiClient(
                Configuration(host=polaris_catalog_url, access_token=options.access_token),
            )
        elif has_client_secret:
            return lambda: ApiClient(
                Configuration(host=polaris_catalog_url, username=options.client_id, password=options.client_secret),
            )
        else:
            raise Exception("Please provide credentials via --client-id & --client-secret or via --access-token")


if __name__ == '__main__':
    PolarisCli.execute()
