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
ARG POLARIS_HOST=polaris
ENV POLARIS_HOST=$POLARIS_HOST
ENV SPARK_HOME=/opt/spark
ENV CURRENT_SCALA_VERSION='2.12'
ENV LANGUAGE='en_US:en'

USER root
RUN apt update
RUN apt-get install -y diffutils wget curl
RUN mkdir -p /home/spark &&  \
    chown -R spark /home/spark && \
    mkdir -p /tmp/polaris-regtests && \
    chown -R spark /tmp/polaris-regtests
RUN mkdir /opt/spark/conf && chmod -R 777 /opt/spark/conf

USER spark

WORKDIR /home/spark/polaris

COPY --chown=spark ./v3.5 /home/spark/polaris/v3.5

# /home/spark/regtests might not be writable in all situations, see https://github.com/apache/polaris/pull/205
USER root
RUN chmod -R go+rwx /home/spark/polaris
RUN chmod -R 777 ./v3.5/regtests
USER spark

ENTRYPOINT ["./v3.5/regtests/run.sh"]
