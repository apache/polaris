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

# Apache Polaris Python Package
The Apache Polaris Python package provides a client for interacting with the Apache Polaris REST APIs, including management, Iceberg Catalog, and Polaris Catalog APIs. It enables users to manage and query data catalogs programmatically from Python applications.

## Development

### Prerequisites
- Python 3.9 or later
- poetry >= 2.0

### Installation
First we need to generate the OpenAPI client code from the OpenAPI specification. 
```
make regenerate-client
```
Install the project with test dependencies:
```
poetry install --all-extras
```

### Auto-formatting and Linting
```
make lint
```

### Running Integration Tests
```
make test-integration
```