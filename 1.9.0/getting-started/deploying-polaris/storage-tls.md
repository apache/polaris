---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: Accessing Storage with TLS and Self-Signed Certificates
linkTitle: Storage with TLS
type: docs
weight: 300
---

Sometimes the storage targeted by the Polaris Server is expected to be accessed over TLS but the
storage system's certificate does not have a trust chain leading to a well-known root. Often
such a certificate is simply self-signed.

In this situation the JVM inside the Polaris Server will need to be configured with a custom
trust store containing the self-signed certificate or its CA certificate.

The example below assumes using a self-signed certificate for storage and `docker` for running Polaris.

# Creating a Custom Trust Store

* Take an existing java trust store (with the usual root certificates) and make a local copy.
* Add the storage system's certificate to it.

```shell
keytool -importcert -file STORAGE_CERT.pem -keystore cacerts -alias STORAGE_CERT
```

Here, `STORAGE_CERT.pem` is the file containing the storage system's certificate; `cacerts` is the name of a custom
trust store file to be used by Polaris.

# Running Polaris with a Custom Trust Store

Map the location of the custom trust store to a local path inside the Polaris container and instruct the Polaris JVM to use it.

```shell
docker run -p 8181:8181 \
 -v /path/to/dir-containing-cacerts:/opt/tls \
 -e JAVA_OPTS_APPEND='-Djavax.net.ssl.trustStore=/opt/tls/cacerts' \
 apache/polaris:latest
```

Of course, add other Polaris and/or docker options as appropriate for your environment.

After this, create a Polaris catalog as usual. Note that the storage `endpoint` property in the catalog probably needs
to use the `https` URI scheme.