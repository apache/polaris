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

FROM ubuntu:25.10 AS hugo

ENV LANGUAGE='en_US:en' \
    HUGO_VERSION='0.157.0'

# Install all packages and Hugo.
# Hugo v0.157.0 isn't available via Ubuntu's package repos, but as a download from the project.
# The package for the currently running system architecture (amd64/arm64) is being downloaded and installed.
# Ubuntu provides recent Hugo versions via `snap`, but that's way to complicated to use in a Docker container.
RUN apt-get update && \
    apt-get install --yes --no-install-recommends golang asciidoctor npm curl git && \
    curl --fail-with-body --location --output hugo.deb "https://github.com/gohugoio/hugo/releases/download/v${HUGO_VERSION}/hugo_extended_withdeploy_${HUGO_VERSION}_linux-$(go env GOARCH).deb" && \
    dpkg --install ./hugo.deb && \
    rm -rf /var/lib/apt/lists/* && \
    npm install --global http-server && \
    mkdir -p /polaris/site/resources

COPY _run_in_docker.sh /hugo/run

EXPOSE 1313 8080

ENTRYPOINT ["/hugo/run"]
