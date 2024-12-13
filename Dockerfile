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

# Base Image
# Use a non-docker-io registry, because pulling images from docker.io is
# subject to aggressive request rate limiting and bandwidth shaping.
FROM registry.access.redhat.com/ubi9/openjdk-21:1.21-3.1733995526 AS build
ARG ECLIPSELINK=false
ARG ECLIPSELINK_DEPS

# Copy the REST catalog into the container
COPY --chown=default:root . /app

# Set the working directory in the container, nuke any existing builds
WORKDIR /app
RUN rm -rf build

# Build the rest catalog
RUN ./gradlew --no-daemon --info ${ECLIPSELINK_DEPS+"-PeclipseLinkDeps=$ECLIPSELINK_DEPS"} -PeclipseLink=$ECLIPSELINK clean :polaris-quarkus-service:build -x test

FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:1.21-1.1733995527

LABEL org.opencontainers.image.source=https://github.com/apache/polaris
LABEL org.opencontainers.image.description="Apache Polaris (incubating)"
LABEL org.opencontainers.image.licenses=Apache-2.0

ENV LANGUAGE='en_US:en'

USER root
RUN groupadd --gid 10001 polaris \
      && useradd --uid 10000 --gid polaris polaris \
      && chown -R polaris:polaris /opt/jboss/container \
      && chown -R polaris:polaris /deployments

USER polaris
WORKDIR /home/polaris
ENV USER=polaris
ENV UID=10000
ENV HOME=/home/polaris

# We make four distinct layers so if there are application changes the library layers can be re-used
COPY --from=build --chown=polaris:polaris /app/dropwizard/service/build/quarkus-app/lib/ /deployments/lib/
COPY --from=build --chown=polaris:polaris /app/dropwizard/service/build/quarkus-app/*.jar /deployments/
COPY --from=build --chown=polaris:polaris /app/dropwizard/service/build/quarkus-app/app/ /deployments/app/
COPY --from=build --chown=polaris:polaris /app/dropwizard/service/build/quarkus-app/quarkus/ /deployments/quarkus/

EXPOSE 8181
EXPOSE 8182

ENV AB_JOLOKIA_OFF=""
ENV JAVA_APP_JAR="/deployments/quarkus-run.jar"
