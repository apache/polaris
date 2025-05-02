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

# Polaris Admin Tool

The Polaris Admin Tool is a maintenance tool for performing administrative tasks on the Polaris server.

## Prerequisites

Polaris admin tool requires a Java SE 21 or higher to run.

## Getting help

To get help, simple run (in a command line where you extracted the Polaris tool archive):

```
./run.sh help
```

## Bootstrap

You can bootstrap realms and root principal credentials with:

```
./run.sh bootstrap
```

providing all required arguments.

## Purge

You can purge realms and all associated entities with:

```
./run.sh purge
```

## Documentation

For more details, please take a look on the documentation: https://polaris.apache.org/in-dev/unreleased/admin-tool/