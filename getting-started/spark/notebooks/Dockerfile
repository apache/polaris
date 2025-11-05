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

ENV PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:/home/spark/venv/lib/python3.10/site-packages"
ENV PYSPARK_PYTHON=/home/spark/venv/bin/python

USER root

RUN apt-get update -y && \
    apt-get install -y python3-venv && \
    mkdir -p /home/spark && \
    chown -R spark /home/spark

USER spark

WORKDIR /home/spark

COPY --chown=spark client /home/spark/client
COPY --chown=spark regtests/requirements.txt /tmp
COPY --chown=spark regtests/notebook_requirements.txt /tmp

RUN python3 -m venv /home/spark/venv && \
    . /home/spark/venv/bin/activate && \
    pip install -r /tmp/requirements.txt -r /tmp/notebook_requirements.txt && \
    cd client/python && \
    poetry lock && \
    poetry install --all-extras

EXPOSE 8888
CMD ["/home/spark/venv/bin/jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=''"]
