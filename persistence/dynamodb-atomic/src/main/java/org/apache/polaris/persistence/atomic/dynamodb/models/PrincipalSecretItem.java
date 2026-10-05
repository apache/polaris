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
package org.apache.polaris.persistence.atomic.dynamodb.models;

import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_CLIENT_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_MAIN_SECRET_HASH;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_PRINCIPAL_ID;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_SECONDARY_SECRET_HASH;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.A_SECRET_SALT;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.PK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.SK;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.SK_SECRET;
import static org.apache.polaris.persistence.atomic.dynamodb.DynamoDbConstants.secretPk;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.n;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.num;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.s;
import static org.apache.polaris.persistence.atomic.dynamodb.models.EntityItem.str;

import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Maps {@link PolarisPrincipalSecrets} to/from the DynamoDB "principal-secret item". Mirrors {@code
 * ModelPrincipalAuthenticationData} on the JDBC side.
 *
 * <p>Keyed by {@code PK = realmId#SECRET#clientId}, {@code SK = "SECRET"} so a principal's secrets
 * are a single-partition point read/write by client id. Only the salted <em>hashes</em> and the
 * salt are persisted — never the raw secrets — matching the relational backend.
 */
public final class PrincipalSecretItem {

  private PrincipalSecretItem() {}

  public static Map<String, AttributeValue> toItem(
      String realmId, PolarisPrincipalSecrets secrets) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(PK, s(secretPk(realmId, secrets.getPrincipalClientId())));
    item.put(SK, s(SK_SECRET));
    item.put(A_PRINCIPAL_ID, n(secrets.getPrincipalId()));
    item.put(A_CLIENT_ID, s(secrets.getPrincipalClientId()));
    item.put(A_SECRET_SALT, s(secrets.getSecretSalt()));
    item.put(A_MAIN_SECRET_HASH, s(secrets.getMainSecretHash()));
    item.put(A_SECONDARY_SECRET_HASH, s(secrets.getSecondarySecretHash()));
    return item;
  }

  /**
   * Reconstruct the secrets from a stored item; raw secrets stay null (hashes are authoritative).
   */
  public static PolarisPrincipalSecrets toSecrets(Map<String, AttributeValue> item) {
    return new PolarisPrincipalSecrets(
        num(item, A_PRINCIPAL_ID),
        str(item, A_CLIENT_ID),
        null,
        null,
        str(item, A_SECRET_SALT),
        str(item, A_MAIN_SECRET_HASH),
        str(item, A_SECONDARY_SECRET_HASH));
  }
}
