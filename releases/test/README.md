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

# Apache Polaris Release Infrastructure Tests

## Prerequisites

A GPG key usable for signing is required, and a GPG agent.

See also [bats docs](https://bats-core.readthedocs.io/en/stable/index.html)

## Setup

```bash
cd releases/test

./setup-tests.sh
```

Run the `export PATH=...` mentioned in the output.

## Run tests

```bash
bats test/
```

## IntelliJ tip

Tell IntelliJ to handle `*.bats` files as shell scripts.
