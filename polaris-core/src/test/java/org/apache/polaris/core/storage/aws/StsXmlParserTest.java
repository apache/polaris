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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.polaris.core.storage.AccessConfig;
import org.junit.jupiter.api.Test;

public class StsXmlParserTest {

  private static final String SAMPLE_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
          + "<ns2:AssumeRoleResponse xmlns:ns2=\"none\">"
          + "<AssumeRoleResult>"
          + "<Credentials>"
          + "<AccessKeyId>ASIA9BC3BEA6F9D5B811</AccessKeyId>"
          + "<Expiration>2025-10-15T02:38:50Z</Expiration>"
          + "<SecretAccessKey>sekrit</SecretAccessKey>"
          + "<SessionToken>the-token-value</SessionToken>"
          + "</Credentials>"
          + "</AssumeRoleResult>"
          + "</ns2:AssumeRoleResponse>";

  @Test
  public void testParseValidXml() throws Exception {
    AccessConfig cfg = StsXmlParser.parseToAccessConfig(SAMPLE_XML);
    assertThat(cfg.get(org.apache.polaris.core.storage.StorageAccessProperty.AWS_KEY_ID))
        .isEqualTo("ASIA9BC3BEA6F9D5B811");
    assertThat(cfg.get(org.apache.polaris.core.storage.StorageAccessProperty.AWS_SECRET_KEY))
        .isEqualTo("sekrit");
    assertThat(cfg.get(org.apache.polaris.core.storage.StorageAccessProperty.AWS_TOKEN))
        .isEqualTo("the-token-value");
    assertThat(cfg.expiresAt()).isPresent();
  }

  @Test
  public void testParseMissingAccessKeyThrows() {
    String bad =
        "<AssumeRoleResponse><AssumeRoleResult><Credentials></Credentials></AssumeRoleResult></AssumeRoleResponse>";
    try {
      StsXmlParser.parseToAccessConfig(bad);
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessageContaining("No AccessKeyId");
      return;
    } catch (Exception e) {
      // fail
      throw new AssertionError("unexpected exception", e);
    }
    throw new AssertionError("expected IllegalArgumentException for missing AccessKeyId");
  }
}
