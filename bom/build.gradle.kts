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

    api(project(":polaris-container-spec-helper"))
    api(project(":polaris-immutables"))
    api(project(":polaris-misc-types"))
    api(project(":polaris-version"))
    api(project(":polaris-persistence-varint"))

    api(project(":polaris-config-docs-annotations"))
    api(project(":polaris-config-docs-generator"))

    api(project(":polaris-core"))
    api(project(":polaris-service-common"))

    api(project(":polaris-eclipselink"))
    api(project(":polaris-relational-jdbc"))

    api(project(":polaris-quarkus-admin"))
    api(project(":polaris-quarkus-common"))
    api(project(":polaris-quarkus-test-commons"))
    api(project(":polaris-quarkus-defaults"))
    api(project(":polaris-quarkus-server"))
    api(project(":polaris-quarkus-service"))
    api(project(":polaris-quarkus-spark-tests"))

    api(project(":polaris-tests"))
  }
}
