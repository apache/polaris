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

# Polaris object store operations

API and implementations to perform long-running operations against object stores.

Functionalities to scan an object store and to purge files are separated. Filter mechanisms are used to
select the files to be deleted (purged).

There are implementations to identify the files referenced by a particular Iceberg table or view metadata, including
statistics files, manifest lists of all snapshots, the manifest files and the data/delete files.

The file operations perform no effort to identify duplicates during the identification of files referenced by
a table or view metadata.
This means that, for example, a data file referenced in multiple manifest files will be returned twice.

Purge operations are performed in one or multiple bulk delete operations.
The implementation takes care of not including the same file more than once within a single bulk delete operation.

One alternative implementation purges all files within the base location of a table or view metadata.

All implemented operations are designed to be resilient against failures as those are expected to be run as
maintenance operations or as part of such.
The operations are implemented to continue in case of errors and eventually succeed instead of failing eagerly.
Maintenance operations are usually not actively observed, and manually fixing consistency issues in object
stores is not a straightforward task for users.

# Potential future enhancements

The operations provided by `FileOperations` are meant for maintenance operations, which are not
time- or performance-critical.
It is more important that the operations are resilient against failures, do not add unnecessary CPU or heap pressure
and eventually succeed.
Further, maintenance operations should not eat up too much I/O bandwidth to not interfere with other user-facing
operations.

Depending on the overall load of the system, it might be worth running some operations in parallel.

# Code architecture

The code is split in two modules. One for the (Polaris internal) API interfaces and one for the implementations.

Tests against various object store implementations are included as unit tests using an on-heap object-store-mock
and as integration tests against test containers for S3, GCS and ADLS.
The object-store-mock used in unit tests is also used to validate the low heap-pressure required by the
implementations. 

The actual object store interaction of the current implementation is delegated to Iceberg `FileIO` implementations.
Only `FileIO` implementations that support prefix-operations (`SupportsPrefixOperations` interface) and
bulk-operations (`SupportsBulkOperations` interface) are currently supported.
The `FileIO` implementations `S3FileIO`, `GCSFileIO` and `ADLSFileIO` support both.
Beside the necessary `FileIO` usage in `FileOperationsFactory`, none of the API functions refer to `FileIO`
to allow the API to be implemented against other, more tailored object store backend implementations.
