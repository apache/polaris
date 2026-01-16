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
title: Migrating from Existing Iceberg Catalogs
linkTitle: Catalog Migration
type: docs
weight: 300
---

There are two ways to migrate an existing Iceberg catalog to Polaris:
1. Using the [Iceberg Catalog Migrator tool](https://github.com/apache/polaris-tools/blob/main/iceberg-catalog-migrator/README.md): A command-line tool to migrate Iceberg tables from one Iceberg catalog to another. This tool works with any existing Iceberg catalog including Polaris.
2. Using the [Polaris Synchronizer tool](https://github.com/apache/polaris-tools/blob/main/polaris-synchronizer/README.md): A tool to migrate entities from one Polaris instance to another. This tool is specific to Polaris.

Both of these tools are available in the [Polaris-Tools repository](https://github.com/apache/polaris-tools). Please refer to the relevant README.md documentation for more information.
