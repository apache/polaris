# Base Image
FROM gradle:8.6-jdk21 as build

# Copy the REST catalog into the container
COPY . /app

# Set the working directory in the container, nuke any existing builds
WORKDIR /app
RUN rm -rf build

# Build the rest catalog
RUN gradle --no-daemon --info shadowJar

FROM openjdk:21
WORKDIR /app
COPY --from=build /app/polaris-service/build/libs/polaris-service-1.0.0-all.jar /app
COPY --from=build /app/polaris-server.yml /app

EXPOSE 8181

# Run the resulting java binary
CMD ["java", "-jar", "/app/polaris-service-1.0.0-all.jar", "server", "polaris-server.yml"]
