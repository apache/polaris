#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Use a non-docker-io registry, because pulling images from docker.io is
# subject to aggressive request rate limiting and bandwidth shaping.
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:1.20-2.1721752928

COPY lib/ /app/lib/
COPY bin/ /app/bin/
COPY polaris-server.yml /app

WORKDIR /app

EXPOSE 8181

# Run the resulting java binary
ENTRYPOINT ["/app/bin/polaris-service"]
CMD ["server", "polaris-server.yml"]
