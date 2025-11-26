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

FROM ubuntu:24.04 AS hugo

ENV LANGUAGE='en_US:en'

RUN apt-get update && \
    apt-get install --yes --no-install-recommends golang hugo asciidoctor npm curl git && \
    rm -rf /var/lib/apt/lists/* && \
    npm install --global http-server && \
    mkdir -p /polaris/site/resources

COPY _run_in_docker.sh /hugo/run

EXPOSE 1313 8080

ENTRYPOINT ["/hugo/run"]
