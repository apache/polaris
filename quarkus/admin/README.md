# Polaris Admin Tool

This module contains a maintenance tool for performing administrative tasks on the Polaris database.
It is a Quarkus application that can be used to perform various maintenance tasks targeting the
Polaris database directly.

## Archive distribution

Building this module will create a zip/tar distribution with the Polaris server.

To build the distribution, you can use the following command:

```shell
./gradlew :polaris-quarkus-admin:build
```

You can manually unpack and run the distribution archives:

```shell
cd quarkus/admin/build/distributions
unzip polaris-quarkus-admin-<version>.zip
cd polaris-quarkus-admin-<version>
java -jar polaris-quarkus-admin-<version>-runner.jar
```

## Docker image

To also build the Docker image, you can use the following command:

```shell
./gradlew :polaris-quarkus-admin:assemble -Dquarkus.container-image.build=true
```

## Running the Admin Tool

The admin tool can be run from the command line using the following command:

```shell
java -jar polaris-quarkus-admin-<version>-runner.jar --help
```

Using the Docker image, you can run the admin tool with the following command:

```shell
docker run --rm -it apache/polaris-admin-tool:<version> --help
```