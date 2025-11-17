---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance

# Getting Started with Apache Polaris (Binary Distribution)

Quickly start Apache Polaris using the pre-built binary distribution.

## Prerequisites

Java SE 21 or later. Verify with:

```bash
java -version
```

## Step 1: Download and Extract the Binary Distribution

1. Visit the Apache Polaris downloads page: https://polaris.apache.org/downloads/

2. Download and extract the latest binary (example):

```bash
curl -L https://downloads.apache.org/incubator/polaris/1.0.0-incubating/polaris-bin-1.0.0-incubating.tgz | tar xz
cd polaris-distribution-1.0.0-incubating
```

## Step 2: Configure Polaris (Optional)

Configure the server using environment variables or system properties. Example:

```bash
# Configure server port
export POLARIS_JAVA_OPTS="-Dquarkus.http.port=8080"

# Multiple options (combine in a single assignment)
export POLARIS_JAVA_OPTS="-Xms512m -Xmx1g -Dquarkus.http.port=8080"
```

You can also pass `POLARIS_JAVA_OPTS` when invoking `bin/server`.

## Step 3: Start the Polaris Server

Start the server:

```bash
bin/server
```

By default the server listens on `http://localhost:8181` and exposes health/metrics endpoints under `/q`.

## Step 4: Verify the Server is Running

Check the health endpoint:

```bash
curl http://localhost:8181/q/health
```

You should see a response indicating the server is healthy.

## Step 5: Stop the Polaris Server

Stop the server with `Ctrl+C`, or terminate the process:

```bash
pkill -f "java.*quarkus-run.jar"
```

## Using the Admin Tool

The distribution includes an admin tool for bootstrapping and maintenance:

```bash
bin/admin --help              # Show admin commands
bin/admin bootstrap -h        # Show bootstrap help
bin/admin purge -h            # Show purge help
```

For additional context, see the distribution README: `runtime/distribution/README.md`.

## Other Getting Started Options

For building and running Polaris from source, see the [Quickstart guide](./quick-start/).

For Docker Compose examples and other deployment options, see https://polaris.apache.org.

### Getting Help

Documentation: https://polaris.apache.org
GitHub Issues: https://github.com/apache/polaris/issues
Slack: https://join.slack.com/t/apache-polaris/shared_invite
bin/admin purge -h            # Show purge help
```

For more details, see the [distribution README](https://github.com/apache/polaris/blob/main/runtime/distribution/README.md).
>>>>>>> 0d7f5e71 (Address reviewer feedback on binary distribution docs)

The binary distribution includes an admin tool for administrative and maintenance tasks:

```bash
bin/admin --help
bin/admin bootstrap -h
bin/admin purge -h
```

## Next Steps

For instructions on building and running Polaris from source code, see the [Quickstart]({{% relref "quickstart" %}}) guide.
