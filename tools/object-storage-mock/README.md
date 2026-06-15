<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Object Storage Mock

`polaris-object-storage-mock` provides an embeddable HTTP server that looks like
cloud object storage to client code. It implements the subset of S3, Google Cloud
Storage, Azure Data Lake Storage Gen2, and AWS STS behavior that Polaris tests
need, while keeping the actual object backing store under test control.

The mock is intended for tests that should exercise real storage client
libraries, request paths, response metadata, conditional requests, listing
pagination, and error handling without depending on external cloud services or
containerized storage services.

## Why Not Just Use a File System or a Map?

A file-system directory or an in-memory `Map<String, byte[]>` can store bytes,
but it does not behave like an object storage service. Object storage clients
interact with an HTTP API, not directly with files or maps, and much of the
behavior Polaris cares about lives in that protocol layer:

- Provider-specific URLs, query parameters, headers, response bodies, and status
  codes.
- Object metadata such as ETags, content types, content lengths, last-modified
  timestamps, and storage classes.
- Conditional reads using headers such as `If-Match` and `If-None-Match`.
- Prefix listing, continuation tokens, page sizes, and provider-specific list
  response shapes.
- Writes, appends, deletes, and batch deletes as real SDKs issue them.
- STS `AssumeRole` responses for code paths that obtain temporary S3
  credentials.

This module separates those concerns. The HTTP resources translate provider
protocols into a small bucket abstraction, and tests decide how each bucket
behaves. A bucket can be heap-backed, generated on demand, wrapped by
interceptors, or connected to a custom fixture.

## What This Enables

The mock lets tests use the same SDKs and configuration paths that production
code uses. For example, tests can point an AWS S3 client, Google Cloud Storage
client, Azure Data Lake Storage Gen2 client, or Iceberg file IO configuration at
the mock server and verify behavior through that client rather than bypassing it
with direct fixture access.

This is useful for:

- Fast unit and integration tests that do not require cloud credentials.
- Cross-provider tests that reuse the same logical objects through S3, GCS, and
  ADLS Gen2 APIs.
- Tests that need large synthetic listings without materializing all objects in
  memory.
- Tests that need precise failures, metadata, access checks, or write
  interception.
- Tests for Polaris storage configuration that need real endpoint URLs.

## Basic Usage

Create one or more buckets, start the server, and configure a client with the
provider-specific endpoint returned by the server:

```java
import static org.apache.polaris.objectstoragemock.HeapStorageBucket.newHeapStorageBucket;

import java.util.Map;
import org.apache.polaris.objectstoragemock.Bucket;
import org.apache.polaris.objectstoragemock.ObjectStorageMock;
import org.apache.polaris.objectstoragemock.ObjectStorageMock.MockServer;

Bucket bucket = newHeapStorageBucket().bucket();

try (MockServer server =
    ObjectStorageMock.builder().putBuckets("warehouse", bucket).build().start()) {
  // Configure S3 clients with server.getS3BaseUri().
  // Configure GCS clients with server.getGcsBaseUri().
  // Configure ADLS Gen2 clients with server.getAdlsGen2BaseUri().
  // Configure STS clients with server.getStsEndpointURI().
}
```

For S3 and Iceberg, `MockServer.icebergProperties()` returns a minimal property
set that points S3 file IO at the mock endpoint and enables path-style access:

```java
try (MockServer server =
    ObjectStorageMock.builder()
        .putBuckets("warehouse", newHeapStorageBucket().bucket())
        .build()
        .start()) {
  Map<String, String> properties = server.icebergProperties();
}
```

When configuring S3 clients directly, use path-style access. The mock exposes
buckets as path segments and does not implement virtual-host-style bucket
routing.

## Bucket Backends

The core test-facing type is `Bucket`. It contains handlers for retrieving,
listing, updating, and deleting objects:

- `object()` returns a `MockObject` for a key.
- `lister()` streams keys and metadata for list operations.
- `updater()` receives writes and commits resulting objects.
- `deleter()` removes keys.

`HeapStorageBucket` is the simplest backend. It stores objects in memory and is
useful when a test needs ordinary read-after-write behavior.

For more controlled tests, build a `Bucket` directly. This allows synthetic
listings, custom metadata, write capture, and targeted failures without
preloading a backing store.

`InterceptingBucket` wraps another bucket and lets a test override selected
operations while falling back to the wrapped bucket for everything else. This is
useful when most behavior should be normal but one key, prefix, or operation
needs special handling.

## Supported Protocol Surfaces

The mock implements only the storage API surface needed by Polaris tests. It is
not a complete replacement for S3, GCS, ADLS Gen2, or STS.

Supported behavior includes:

- S3 bucket listing, object listing, head, get, put, delete, batch delete, and
  STS assume-role responses.
- GCS object listing, metadata reads, media reads, writes, and deletes.
- ADLS Gen2 path listing, properties, reads, creates, appends, flushes, and
  deletes.
- Range and conditional request handling where implemented by the resources.

Unsupported operations generally return a not-implemented response or are absent
from the mock API. Add only the protocol behavior a test actually needs.
