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

FROM docker.io/apache/spark:3.5.6-java17

ARG POLARIS_HOST=polaris \
    CURRENT_SCALA_VERSION=2.12

ENV POLARIS_HOST=${POLARIS_HOST} \
    CURRENT_SCALA_VERSION=${CURRENT_SCALA_VERSION}

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends diffutils wget curl && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /home/spark /tmp/polaris-regtests /opt/spark/conf && \
    chown -R spark:spark /home/spark /tmp/polaris-regtests && \
    chmod -R 777 /opt/spark/conf

WORKDIR /home/spark/polaris

COPY --chown=spark:spark ./v3.5 /home/spark/polaris/v3.5

# /home/spark/regtests might not be writable in all situations, see https://github.com/apache/polaris/pull/205
RUN chmod -R 777 /home/spark/polaris/v3.5/regtests

USER spark

ENTRYPOINT ["./v3.5/regtests/run.sh"]
