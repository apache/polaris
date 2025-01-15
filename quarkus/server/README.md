# Polaris Quarkus Server

This module contains the Quarkus-based Polaris server main artifact.

Building this module will create a zip/tar distribution with the Polaris server.

To also build the Docker image, you can use the following command (a running Docker daemon is
required):

```shell
./gradlew :polaris-quarkus-server:assemble -Dquarkus.container-image.build=true
```

If you need to customize the Docker image, for example to push to a local registry, you can use the
following command:

```shell
./gradlew :polaris-quarkus-server:build -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.registry=localhost:5001 \
  -Dquarkus.container-image.group=apache \
  -Dquarkus.container-image.name=polaris-local
```