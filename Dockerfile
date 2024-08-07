#
# Copyright (c) 2024 Snowflake Computing Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Base Image
# Use a non-docker-io registry, because pulling images from docker.io is
# subject to aggressive request rate limiting and bandwidth shaping.
FROM registry.access.redhat.com/ubi9/openjdk-21:1.20-2.1721752936 as build
ARG ECLIPSELINK=false

# Copy the REST catalog into the container
COPY --chown=default:root . /app

# Set the working directory in the container, nuke any existing builds
WORKDIR /app
RUN rm -rf build

# Build the rest catalog
RUN ./gradlew --no-daemon --info -PeclipseLink=$ECLIPSELINK clean shadowJar

FROM registry.access.redhat.com/ubi9/openjdk-21-runtime:1.20-2.1721752928
WORKDIR /app
COPY --from=build /app/polaris-service/build/libs/polaris-service-1.0.0-all.jar /app
COPY --from=build /app/polaris-server.yml /app

EXPOSE 8181

# Run the resulting java binary
ENTRYPOINT ["java", "-jar", "/app/polaris-service-1.0.0-all.jar"]
CMD ["server", "polaris-server.yml"]
