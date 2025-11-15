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

FROM docker.io/apache/spark:3.5.6-java17-python3

ARG POLARIS_HOST=polaris

ENV POLARIS_HOST=${POLARIS_HOST} \
    PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip"

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends diffutils wget curl python3.10-venv jq && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /home/spark /tmp/polaris-regtests /opt/spark/conf && \
    chown -R spark:spark /home/spark /tmp/polaris-regtests && \
    chmod -R 777 /opt/spark/conf

COPY --chown=spark:spark ./regtests/setup.sh ./regtests/pyspark-setup.sh ./regtests/requirements.txt /home/spark/polaris/regtests/
COPY --chown=spark:spark ./client/python /home/spark/polaris/client/python
COPY --chown=spark:spark ./polaris /home/spark/polaris/polaris
COPY --chown=spark:spark ./spec /home/spark/polaris/spec
COPY --chown=spark:spark ./regtests /home/spark/polaris/regtests

# /home/spark/regtests might not be writable in all situations, see https://github.com/apache/polaris/pull/205
RUN chmod -R go+rwx /home/spark/polaris

USER spark

WORKDIR /home/spark/polaris

RUN python3 -m venv /home/spark/polaris/polaris-venv && \
    . /home/spark/polaris/polaris-venv/bin/activate && \
    pip install -r /home/spark/polaris/regtests/requirements.txt && \
    cd /home/spark/polaris/client/python && \
    poetry install && \
    deactivate && \
    /home/spark/polaris/regtests/setup.sh

ENTRYPOINT ["./regtests/run.sh"]
