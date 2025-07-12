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

"""
Simple class to test OAuth endpoints in the Polaris Service.
"""
import argparse
import requests


def main(base_uri, client_id, client_secret):
    """
    Args:
        base_uri:      The Base URI (ex: http://localhost:8181)
        client_id:     The Client ID of the OAuth2 Client to Use
        client_secret: The Client Secret of the OAuth2 Client to Use
    """
    oauth_uri = base_uri + '/api/catalog/v1/oauth/tokens'
    headers = {} # may have client id / secret in the future
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    r = requests.post(
        oauth_uri,
        headers=headers,
        data=payload)
    data = r.json()

    if 'error' in data:
        # Cannot continue at this point
        print("Unable to obtain an OAuth Token, see error below")
        print(data)
        return

    # Get the actual token and remove out hint/version
    token = data['access_token']
    print("Successfully obtained OAuth token\n\n")

    # Let's call a sample endpoint. The "/config" one seems like the best bet
    headers = {"Authorization": f"Bearer {token}"}
    config_uri = base_uri + "/api/catalog/v1/config"
    r = requests.get(config_uri, headers=headers)
    print(r.text)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-uri", help="The Base Polaris Server URI (ex: http://localhost:8181", type=str)
    parser.add_argument("--client-id", help="The Client ID of the OAuth2 Client Integration", type=str)
    parser.add_argument("--client-secret", help="The Client Secret of the OAuth2 Client Integration", type=str)
    args = parser.parse_args()
    main(args.base_uri, args.client_id, args.client_secret)
