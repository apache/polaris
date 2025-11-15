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

ENDPOINT=$1
# "invalidKey" in combination with SigV4 means "public" access
KEY_ID=${2:-"invalidKey"}
SECRET=${3:-"secret"}
SLEEP=${4:-"1"}

if [ -z "$ENDPOINT" ]; then
  echo Endpoint must be provided
  exit 1
fi

# Make up to 30 attempts to list buckets. Success means the service is available
for i in `seq 1 30`; do
  echo "Listing buckets at $ENDPOINT"
  curl --user "$KEY_ID:$SECRET" --aws-sigv4 "aws:amz:us-west-1:s3" $ENDPOINT
  if [[ "$?" == "0" ]]; then
    echo
    echo "$ENDPOINT is available"
    break
  fi
  echo "Sleeping $SLEEP ..."
  sleep $SLEEP
done