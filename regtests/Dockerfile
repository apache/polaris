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

FROM docker.io/python:3.13-slim
ARG POLARIS_HOST=polaris
ENV POLARIS_HOST=$POLARIS_HOST

RUN groupadd polaris && \
    useradd -m -g polaris -s /bin/bash polaris

WORKDIR /home/polaris/regtests

COPY ./polaris-reg-test /home/polaris/polaris
COPY . /home/polaris/regtests

RUN apt-get update && \
    apt-get install -y procps openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/* && \
    chown -R polaris:polaris /home/polaris && \
    mkdir /tmp/polaris-regtests && \
    chown -R polaris:polaris /tmp/polaris-regtests

USER polaris

ENTRYPOINT ["/home/polaris/regtests/run.sh"]
