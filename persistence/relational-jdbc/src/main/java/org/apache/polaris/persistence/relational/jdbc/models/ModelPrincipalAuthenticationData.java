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
package org.apache.polaris.persistence.relational.jdbc.models;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;

public class ModelPrincipalAuthenticationData implements Converter<PolarisPrincipalSecrets> {
  // the id of the principal
  private long principalId;

  // the client id for that principal
  private String principalClientId;

  // Hash of mainSecret
  private String mainSecretHash;

  // Hash of secondarySecret
  private String secondarySecretHash;

  private String secretSalt;

  public long getPrincipalId() {
    return principalId;
  }

  public String getPrincipalClientId() {
    return principalClientId;
  }

  public String getSecretSalt() {
    return secretSalt;
  }

  public String getMainSecretHash() {
    return mainSecretHash;
  }

  public String getSecondarySecretHash() {
    return secondarySecretHash;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public PolarisPrincipalSecrets fromResultSet(ResultSet rs) throws SQLException {
    var modelRecord =
        ModelPrincipalAuthenticationData.builder()
            .principalId(rs.getObject("principal_id", Long.class))
            .principalClientId(rs.getObject("principal_client_id", String.class))
            .mainSecretHash(rs.getObject("main_secret_hash", String.class))
            .secondarySecretHash(rs.getObject("secondary_secret_hash", String.class))
            .secretSalt(rs.getObject("secret_salt", String.class))
            .build();

    return toPrincipalAuthenticationData(modelRecord);
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("principal_id", this.principalId);
    map.put("principal_client_id", this.principalClientId);
    map.put("main_secret_hash", this.mainSecretHash);
    map.put("secondary_secret_hash", this.secondarySecretHash);
    map.put("secret_salt", this.secretSalt);
    return map;
  }

  public static final class Builder {
    private final ModelPrincipalAuthenticationData principalAuthenticationData;

    private Builder() {
      principalAuthenticationData = new ModelPrincipalAuthenticationData();
    }

    public Builder principalId(long principalId) {
      principalAuthenticationData.principalId = principalId;
      return this;
    }

    public Builder principalClientId(String principalClientId) {
      principalAuthenticationData.principalClientId = principalClientId;
      return this;
    }

    public Builder secretSalt(String secretSalt) {
      principalAuthenticationData.secretSalt = secretSalt;
      return this;
    }

    public Builder mainSecretHash(String mainSecretHash) {
      principalAuthenticationData.mainSecretHash = mainSecretHash;
      return this;
    }

    public Builder secondarySecretHash(String secondarySecretHash) {
      principalAuthenticationData.secondarySecretHash = secondarySecretHash;
      return this;
    }

    public ModelPrincipalAuthenticationData build() {
      return principalAuthenticationData;
    }
  }

  public static ModelPrincipalAuthenticationData fromPrincipalAuthenticationData(
      PolarisPrincipalSecrets record) {
    if (record == null) return null;

    return ModelPrincipalAuthenticationData.builder()
        .principalId(record.getPrincipalId())
        .principalClientId(record.getPrincipalClientId())
        .secretSalt(record.getSecretSalt())
        .mainSecretHash(record.getMainSecretHash())
        .secondarySecretHash(record.getSecondarySecretHash())
        .build();
  }

  public static PolarisPrincipalSecrets toPrincipalAuthenticationData(
      ModelPrincipalAuthenticationData model) {
    if (model == null) return null;

    return new PolarisPrincipalSecrets(
        model.getPrincipalId(),
        model.getPrincipalClientId(),
        null,
        null,
        model.getSecretSalt(),
        model.getMainSecretHash(),
        model.getSecondarySecretHash());
  }
}
