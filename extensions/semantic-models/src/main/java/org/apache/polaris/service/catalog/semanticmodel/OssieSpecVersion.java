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
package org.apache.polaris.service.catalog.semanticmodel;

import org.apache.iceberg.exceptions.BadRequestException;

/**
 * Supported Apache Ossie specification versions and their bundled JSON Schemas.
 *
 * <p>Schema artifacts are vendored from the <a href="https://github.com/apache/ossie">Apache
 * Ossie</a> repository ({@code core-spec/ossie-schema.json}) and loaded from the classpath at
 * runtime.
 */
enum OssieSpecVersion {
  V0_2_0_DEV0(
      "0.2.0.dev0",
      "org/apache/polaris/service/catalog/semanticmodel/ossie/0.2.0.dev0/ossie-schema.json");

  private final String specVersion;
  private final String schemaResourcePath;

  OssieSpecVersion(String specVersion, String schemaResourcePath) {
    this.specVersion = specVersion;
    this.schemaResourcePath = schemaResourcePath;
  }

  String specVersion() {
    return specVersion;
  }

  String schemaResourcePath() {
    return schemaResourcePath;
  }

  static OssieSpecVersion fromDocumentVersion(String documentVersion) {
    if (documentVersion == null || documentVersion.isBlank()) {
      throw new BadRequestException("Semantic model document must declare a non-empty 'version'");
    }
    for (OssieSpecVersion supported : values()) {
      if (supported.specVersion.equals(documentVersion)) {
        return supported;
      }
    }
    throw new BadRequestException(
        "Unsupported Apache Ossie spec version '%s'. Supported versions: %s",
        documentVersion, supportedVersions());
  }

  private static String supportedVersions() {
    StringBuilder versions = new StringBuilder();
    for (OssieSpecVersion supported : values()) {
      if (versions.length() > 0) {
        versions.append(", ");
      }
      versions.append('\'').append(supported.specVersion).append('\'');
    }
    return versions.toString();
  }
}
