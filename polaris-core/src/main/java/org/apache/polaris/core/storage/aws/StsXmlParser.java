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
package org.apache.polaris.core.storage.aws;

import java.time.Instant;
import java.util.Objects;
import org.apache.polaris.core.storage.AccessConfig;

/** Utility to parse STS AssumeRoleResponse XML (namespaced or not) into an AccessConfig. */
public final class StsXmlParser {
  private StsXmlParser() {}

  public static AccessConfig parseToAccessConfig(String xml) throws Exception {
    Objects.requireNonNull(xml);
    var dbf = javax.xml.parsers.DocumentBuilderFactory.newInstance();
    dbf.setNamespaceAware(true);
    var db = dbf.newDocumentBuilder();
    try (java.io.ByteArrayInputStream in =
        new java.io.ByteArrayInputStream(xml.getBytes(java.nio.charset.StandardCharsets.UTF_8))) {
      org.w3c.dom.Document doc = db.parse(in);
      javax.xml.xpath.XPath xPath = javax.xml.xpath.XPathFactory.newInstance().newXPath();

      String accessKeyId =
          (String)
              xPath.evaluate(
                  "//*[local-name() = 'Credentials']/*[local-name() = 'AccessKeyId']/text()",
                  doc,
                  javax.xml.xpath.XPathConstants.STRING);
      String secretAccessKey =
          (String)
              xPath.evaluate(
                  "//*[local-name() = 'Credentials']/*[local-name() = 'SecretAccessKey']/text()",
                  doc,
                  javax.xml.xpath.XPathConstants.STRING);
      String sessionToken =
          (String)
              xPath.evaluate(
                  "//*[local-name() = 'Credentials']/*[local-name() = 'SessionToken']/text()",
                  doc,
                  javax.xml.xpath.XPathConstants.STRING);
      String expiration =
          (String)
              xPath.evaluate(
                  "//*[local-name() = 'Credentials']/*[local-name() = 'Expiration']/text()",
                  doc,
                  javax.xml.xpath.XPathConstants.STRING);

      if (accessKeyId == null || accessKeyId.isBlank()) {
        throw new IllegalArgumentException("No AccessKeyId found in STS response XML");
      }

      AccessConfig.Builder builder = AccessConfig.builder();
      builder.putCredential(
          org.apache.polaris.core.storage.StorageAccessProperty.AWS_KEY_ID.getPropertyName(),
          accessKeyId.trim());
      if (secretAccessKey != null && !secretAccessKey.isBlank()) {
        builder.putCredential(
            org.apache.polaris.core.storage.StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(),
            secretAccessKey.trim());
      }
      if (sessionToken != null && !sessionToken.isBlank()) {
        builder.putCredential(
            org.apache.polaris.core.storage.StorageAccessProperty.AWS_TOKEN.getPropertyName(),
            sessionToken.trim());
      }
      if (expiration != null && !expiration.isBlank()) {
        try {
          Instant i = Instant.parse(expiration.trim());
          builder.putCredential(
              org.apache.polaris.core.storage.StorageAccessProperty.EXPIRATION_TIME
                  .getPropertyName(),
              String.valueOf(i.toEpochMilli()));
          builder.putCredential(
              org.apache.polaris.core.storage.StorageAccessProperty.AWS_SESSION_TOKEN_EXPIRES_AT_MS
                  .getPropertyName(),
              String.valueOf(i.toEpochMilli()));
          builder.expiresAt(i);
        } catch (Exception e) {
          builder.putExtraProperty(
              org.apache.polaris.core.storage.StorageAccessProperty.EXPIRATION_TIME
                  .getPropertyName(),
              expiration.trim());
        }
      }
      return builder.build();
    }
  }
}
