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
package org.apache.polaris.core.secrets;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Base test class for implementations of UserSecretsManager which can be extended by different
 * implementation-specific unittests.
 */
public abstract class UserSecretsManagerBaseTest {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  /**
   * @return a fresh instance of a UserSecretsManager to use in test cases.
   */
  protected abstract UserSecretsManager newSecretsManager();

  @Test
  public void testBasicSecretStorageAndRetrieval() throws JsonProcessingException {
    UserSecretsManager secretsManager = newSecretsManager();

    PolarisEntity entity1 = new CatalogEntity.Builder().setId(1111L).setName("entity1").build();
    PolarisEntity entity2 = new CatalogEntity.Builder().setId(2222L).setName("entity2").build();

    String secret1 = "sensitivesecret1";
    String secret2 = "sensitivesecret2";

    SecretReference reference1 = secretsManager.writeSecret(secret1, entity1);
    SecretReference reference2 = secretsManager.writeSecret(secret2, entity2);

    // Make sure we can JSON-serialize and deserialize the SecretReference objects.
    String serializedReference1 = DEFAULT_MAPPER.writeValueAsString(reference1);
    String serializedReference2 = DEFAULT_MAPPER.writeValueAsString(reference2);

    SecretReference reassembledReference1 =
        DEFAULT_MAPPER.readValue(serializedReference1, SecretReference.class);
    SecretReference reassembledReference2 =
        DEFAULT_MAPPER.readValue(serializedReference2, SecretReference.class);

    Assertions.assertThat(reassembledReference1).isEqualTo(reference1);
    Assertions.assertThat(reassembledReference2).isEqualTo(reference2);
    Assertions.assertThat(secretsManager.readSecret(reassembledReference1)).isEqualTo(secret1);
    Assertions.assertThat(secretsManager.readSecret(reassembledReference2)).isEqualTo(secret2);
  }

  @Test
  public void testMultipleSecretsForSameEntity() {
    UserSecretsManager secretsManager = newSecretsManager();

    PolarisEntity entity1 = new CatalogEntity.Builder().setId(1111L).setName("entity1").build();

    String secret1 = "sensitivesecret1";
    String secret2 = "sensitivesecret2";

    SecretReference reference1 = secretsManager.writeSecret(secret1, entity1);
    SecretReference reference2 = secretsManager.writeSecret(secret2, entity1);

    Assertions.assertThat(secretsManager.readSecret(reference1)).isEqualTo(secret1);
    Assertions.assertThat(secretsManager.readSecret(reference2)).isEqualTo(secret2);
  }

  @Test
  public void testDeleteSecret() {
    UserSecretsManager secretsManager = newSecretsManager();

    PolarisEntity entity1 = new CatalogEntity.Builder().setId(1111L).setName("entity1").build();

    String secret1 = "sensitivesecret1";

    SecretReference reference1 = secretsManager.writeSecret(secret1, entity1);

    Assertions.assertThat(secretsManager.readSecret(reference1)).isEqualTo(secret1);

    secretsManager.deleteSecret(reference1);
    Assertions.assertThat(secretsManager.readSecret(reference1))
        .as("Deleted secret should return null")
        .isNull();
  }
}
