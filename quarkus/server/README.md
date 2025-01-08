# Polaris Quarkus Server

This module contains the Quarkus-based Polaris server main artifact.

Building this module will create a zip/tar distribution with the Polaris server.

To also build the Docker image, you can use the following command (a running Docker daemon is
required):

```shell
./gradlew :polaris-quarkus-server:build -Dquarkus.container-image.build=true
```
