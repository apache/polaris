/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

plugins { id("polaris-bom") }

description = "Apache Polaris - Bill of Materials (BOM)"

dependencies {
  constraints {
    api(rootProject)
    api(project(":polaris-api-catalog-service"))
    api(project(":polaris-api-iceberg-service"))
    api(project(":polaris-api-management-model"))
    api(project(":polaris-api-management-service"))
    api(project(":polaris-api-s3-sign-service"))

    api(project(":polaris-container-spec-helper"))
    api(project(":polaris-minio-testcontainer"))
    api(project(":polaris-immutables"))
    api(project(":polaris-misc-types"))
    api(project(":polaris-version"))
    api(project(":polaris-persistence-nosql-varint"))

    api(project(":polaris-async-api"))
    api(project(":polaris-async-java"))
    api(project(":polaris-async-vertx"))

    api(project(":polaris-idgen-api"))
    api(project(":polaris-idgen-impl"))
    api(project(":polaris-idgen-spi"))

    api(project(":polaris-nodes-api"))
    api(project(":polaris-nodes-impl"))
    api(project(":polaris-nodes-spi"))

    api(project(":polaris-persistence-nosql-realms-api"))
    api(project(":polaris-persistence-nosql-realms-impl"))
    api(project(":polaris-persistence-nosql-realms-spi"))

    api(project(":polaris-persistence-nosql-api"))
    api(project(":polaris-persistence-nosql-impl"))
    api(project(":polaris-persistence-nosql-benchmark"))
    api(project(":polaris-persistence-nosql-correctness"))
    api(project(":polaris-persistence-nosql-standalone"))
    api(project(":polaris-persistence-nosql-testextension"))

    api(project(":polaris-persistence-nosql-inmemory"))
    api(project(":polaris-persistence-nosql-mongodb"))

    api(project(":polaris-persistence-nosql-maintenance-api"))
    api(project(":polaris-persistence-nosql-maintenance-cel"))
    api(project(":polaris-persistence-nosql-maintenance-spi"))

    api(project(":polaris-config-docs-annotations"))
    api(project(":polaris-config-docs-generator"))

    api(project(":polaris-core"))

    api(project(":polaris-relational-jdbc"))

    api(project(":polaris-extensions-auth-opa"))

    api(project(":polaris-admin"))
    api(project(":polaris-runtime-common"))
    api(project(":polaris-runtime-test-common"))
    api(project(":polaris-runtime-defaults"))
    api(project(":polaris-server"))
    api(project(":polaris-runtime-service"))
    api(project(":polaris-runtime-spark-tests"))

    api(project(":polaris-tests"))
  }
}
