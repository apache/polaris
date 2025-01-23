# Podman/Docker Compose Examples

You can quickly get started with Polaris by playing with the compose examples provided in this
directory. The examples are designed to be run with `docker-compose` or `podman-compose`. Each
example has detailed instructions.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) or [Podman](https://podman.io/docs/installation)
- [Docker Compose](https://docs.docker.com/compose/install/) 
  or [Podman Compose](https://docs.podman.io/en/v5.1.1/markdown/podman-compose.1.html)
- [jq](https://stedolan.github.io/jq/download/) (for some examples)

## Examples

- [In-Memory](./in-memory): A simple example that uses an in-memory metastore, automatically
  bootstrapped.

- [Telemetry](./telemetry): An example that includes Prometheus and Jaeger to collect metrics and
  traces from Polaris. This example automatically creates a `polaris_demo` catalog.

- [Eclipselink](./elipselink): An example that uses an Eclipselink metastore and a Postgres
  database. The realm is bootstrapped with the Polaris Admin tool. This example also creates a
  `polaris_demo` catalog, and offers the ability to run Spark SQL queries. Finally, it shows how to
  attach a debugger to the Polaris server.
