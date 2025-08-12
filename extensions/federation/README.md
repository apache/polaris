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
### Using the `HadoopFederatedCatalogFactory`

This `HadoopFederatedCatalogFactory` module is an independent compilation unit and will be built into the Polaris binary only when the following flag is set in the gradle.properties file:
```
NonRESTCatalogs=HADOOP,<alternates>
```

The other option is to pass it as an argument to the gradle JVM as follows: 
```
./gradlew build -DNonRESTCatalogs=HADOOP
```

Without this flag, the Hadoop factory won't be compiled into Polaris and therefore Polaris will not load the class at runtime, throwing an unsupported exception for federated catalog calls.