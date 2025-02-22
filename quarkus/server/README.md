# Polaris Quarkus Server

This module contains the Quarkus-based Polaris server main artifact.

## Archive distribution

Building this module will create a zip/tar distribution with the Polaris server.

To build the distribution, you can use the following command:

```shell
./gradlew :polaris-quarkus-server:build
```

You can manually unpack and run the distribution archives:

```shell
cd quarkus/server/build/distributions
unzip polaris-quarkus-server-<version>.zip
cd polaris-quarkus-server-<version>
java -jar quarkus-run.jar
```

## Docker image

To also build the Docker image, you can use the following command (a running Docker daemon is
required):

```shell
./gradlew clean :polaris-quarkus-server:assemble -Dquarkus.container-image.build=true --no-build-cache
```

If you need to customize the Docker image, for example to push to a local registry, you can use the
following command:

```shell
./gradlew clean :polaris-quarkus-server:build -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.registry=localhost:5001 \
  -Dquarkus.container-image.group=apache \
  -Dquarkus.container-image.name=polaris-local \
  --no-build-cache
```