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

FROM jupyter/all-spark-notebook:spark-3.5.0

ENV LANGUAGE='en_US:en'

USER root

# Generic table support requires delta 3.2.1
# Install Spark 3.5.6
RUN wget -q https://www.apache.org/dyn/closer.lua/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz?action=download \
    && tar -xzf spark-3.5.6-bin-hadoop3.tgz \
    && mv spark-3.5.6-bin-hadoop3 /opt/spark \
    && rm spark-3.5.6-bin-hadoop3.tgz

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER jovyan

COPY --chown=jovyan client /home/jovyan/client
COPY --chown=jovyan regtests/requirements.txt /tmp
COPY --chown=jovyan plugins/spark/v3.5/spark/build/2.12/libs /home/jovyan/polaris_libs
RUN pip install -r /tmp/requirements.txt
RUN cd client/python && poetry lock && \
    python3 -m poetry install && \
    pip install -e .

WORKDIR /home/jovyan/
