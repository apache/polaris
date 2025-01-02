# Polaris Admin Tool

This module contains a maintenance tool for performing administrative tasks on the Polaris database.
It is a Quarkus application that can be used to perform various maintenance tasks targeting the
Polaris database directly.

Building this module will create a runnable uber-jar that can be executed from the command line.

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
docker run --rm -it polaris-admin-tool:<version> --help
```