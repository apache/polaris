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

FROM docker.io/apache/spark:3.5.1-python3
ARG POLARIS_HOST=polaris
ENV POLARIS_HOST=$POLARIS_HOST
ENV SPARK_HOME=/opt/spark

USER root
RUN apt update
RUN apt-get install -y diffutils wget curl python3.8-venv
RUN mkdir -p /home/spark &&  \
    chown -R spark /home/spark && \
    mkdir -p /tmp/polaris-regtests && \
    chown -R spark /tmp/polaris-regtests
RUN mkdir /opt/spark/conf && chmod -R 777 /opt/spark/conf

USER spark
ENV PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# Copy and run setup.sh separately so that test sources can change, but the setup script run is still cached
WORKDIR /home/spark/regtests
COPY ./setup.sh /home/spark/regtests/setup.sh
COPY ./pyspark-setup.sh /home/spark/regtests/pyspark-setup.sh
COPY ./client/python /home/spark/regtests/client/python
COPY ./polaris-reg-test /home/spark/polaris

RUN python3 -m venv /home/spark/polaris-venv && \
    . /home/spark/polaris-venv/bin/activate && \
    pip install poetry==1.5.0 && \
    deactivate \
RUN ./setup.sh

COPY --chown=spark . /home/spark/regtests

# /home/spark/regtests might not be writable in all situations, see https://github.com/apache/polaris/pull/205
USER root
RUN chmod -R go+rwx /home/spark/regtests
USER spark

ENTRYPOINT ["./run.sh"]
