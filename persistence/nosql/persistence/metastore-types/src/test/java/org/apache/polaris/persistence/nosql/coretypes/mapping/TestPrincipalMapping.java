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

package org.apache.polaris.persistence.nosql.coretypes.mapping;

import static org.apache.polaris.core.entity.PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithInternalProperty;
import static org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj.PRINCIPALS_REF_NAME;

import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.core.DigestUtils;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalObj;
import org.apache.polaris.persistence.nosql.coretypes.principals.PrincipalsObj;
import org.apache.polaris.persistence.nosql.coretypes.realm.RootObj;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@MethodSource("parameters")
public class TestPrincipalMapping extends BaseTestMapping {
  @Override
  public ObjType containerObjType() {
    return PrincipalsObj.TYPE;
  }

  @Override
  public Class<? extends ObjBase> baseObjClass() {
    return null;
  }

  @Override
  public String refName() {
    return PRINCIPALS_REF_NAME;
  }

  @Override
  public boolean isCatalogContent() {
    return false;
  }

  @Override
  public boolean isCatalogRelated() {
    return false;
  }

  @Override
  public boolean isWithStorage() {
    return false;
  }

  @Test
  public void noSecrets() {
    var noPrincipal = buildWithDefaults(RootObj.builder()).build();
    soft.assertThat(EntityObjMappings.maybeObjToPolarisPrincipalSecrets(noPrincipal)).isEmpty();
    var noClientId = buildWithDefaults(PrincipalObj.builder()).build();
    soft.assertThat(EntityObjMappings.maybeObjToPolarisPrincipalSecrets(noClientId)).isEmpty();
  }

  @Test
  public void mappingWithSecrets() {
    var obj =
        buildWithDefaults(PrincipalObj.builder())
            .clientId("client-id")
            .secretSalt("1234")
            .mainSecretHash(DigestUtils.sha256Hex("1st-secret" + ":" + "1234"))
            .secondarySecretHash(DigestUtils.sha256Hex("1st-secret" + ":" + "1234"))
            .build();
    var entity = EntityObjMappings.mapToEntity(obj, 0);
    var entityAsObj =
        EntityObjMappings.mapToObj(
                entity,
                Optional.of(
                    new PolarisPrincipalSecrets(
                        obj.stableId(),
                        "client-id",
                        // secrets are not persisted
                        null,
                        null,
                        obj.secretSalt().orElseThrow(),
                        obj.mainSecretHash().orElseThrow(),
                        obj.secondarySecretHash().orElseThrow())))
            .id(obj.id())
            .build();
    soft.assertThat(entityAsObj).isEqualTo(obj);

    entityAsObj =
        EntityObjMappings.mapToObj(
                entity,
                Optional.of(
                    new PolarisPrincipalSecrets(
                        obj.stableId(),
                        "client-id",
                        "1st-secret",
                        "2nd-secret",
                        obj.secretSalt().orElseThrow(),
                        obj.mainSecretHash().orElseThrow(),
                        obj.secondarySecretHash().orElseThrow())))
            .id(obj.id())
            .build();
    soft.assertThat(entityAsObj).isEqualTo(obj);
  }

  @ParameterizedTest
  @MethodSource("secrets")
  public void principalObjToPolarisPrincipalSecrets(
      PrincipalObj obj, PolarisPrincipalSecrets expected) {
    var principalSecrets = EntityObjMappings.maybeObjToPolarisPrincipalSecrets(obj).orElseThrow();
    soft.assertThat(principalSecrets)
        .extracting(
            PolarisPrincipalSecrets::getPrincipalId,
            PolarisPrincipalSecrets::getPrincipalClientId,
            PolarisPrincipalSecrets::getMainSecret,
            PolarisPrincipalSecrets::getSecondarySecret,
            PolarisPrincipalSecrets::getSecretSalt,
            PolarisPrincipalSecrets::getMainSecretHash,
            PolarisPrincipalSecrets::getSecondarySecretHash)
        .containsExactly(
            expected.getPrincipalId(),
            expected.getPrincipalClientId(),
            // secrets are not persisted
            null, // expected.getMainSecret(),
            null, // expected.getSecondarySecret(),
            expected.getSecretSalt(),
            expected.getMainSecretHash(),
            expected.getSecondarySecretHash());

    principalSecrets = EntityObjMappings.principalObjToPolarisPrincipalSecrets(obj);
    soft.assertThat(principalSecrets)
        .extracting(
            PolarisPrincipalSecrets::getPrincipalId,
            PolarisPrincipalSecrets::getPrincipalClientId,
            PolarisPrincipalSecrets::getMainSecret,
            PolarisPrincipalSecrets::getSecondarySecret,
            PolarisPrincipalSecrets::getSecretSalt,
            PolarisPrincipalSecrets::getMainSecretHash,
            PolarisPrincipalSecrets::getSecondarySecretHash)
        .containsExactly(
            expected.getPrincipalId(),
            expected.getPrincipalClientId(),
            // secrets are not persisted
            null, // expected.getMainSecret(),
            null, // expected.getSecondarySecret(),
            expected.getSecretSalt(),
            expected.getMainSecretHash(),
            expected.getSecondarySecretHash());

    principalSecrets = EntityObjMappings.principalObjToPolarisPrincipalSecrets(obj, expected);
    soft.assertThat(principalSecrets)
        .extracting(
            PolarisPrincipalSecrets::getPrincipalId,
            PolarisPrincipalSecrets::getPrincipalClientId,
            PolarisPrincipalSecrets::getMainSecret,
            PolarisPrincipalSecrets::getSecondarySecret,
            PolarisPrincipalSecrets::getSecretSalt,
            PolarisPrincipalSecrets::getMainSecretHash,
            PolarisPrincipalSecrets::getSecondarySecretHash)
        .containsExactly(
            expected.getPrincipalId(),
            expected.getPrincipalClientId(),
            expected.getMainSecret(),
            expected.getSecondarySecret(),
            expected.getSecretSalt(),
            expected.getMainSecretHash(),
            expected.getSecondarySecretHash());
  }

  static Stream<Arguments> secrets() {
    var salt = "1234";
    return Stream.of(
        Arguments.of(
            buildWithDefaults(PrincipalObj.builder())
                .clientId("client-id")
                .secretSalt(salt)
                .build(),
            new PolarisPrincipalSecrets(0L, "client-id", null, null, salt, null, null)),
        Arguments.of(
            buildWithDefaults(PrincipalObj.builder())
                .clientId("client-id")
                .secretSalt(salt)
                .mainSecretHash(DigestUtils.sha256Hex("1st-secret" + ":" + salt))
                .secondarySecretHash(DigestUtils.sha256Hex("2nd-secret" + ":" + salt))
                .build(),
            new PolarisPrincipalSecrets(
                0L, "client-id", "1st-secret", "2nd-secret", salt, null, null)));
  }

  static Stream<BaseTestParameter> parameters() {
    return Stream.of(
        new BaseTestParameter(
            PolarisEntityType.PRINCIPAL, PolarisEntitySubType.NULL_SUBTYPE, PrincipalObj.TYPE) {
          @Override
          public PrincipalObj.Builder objBuilder() {
            return PrincipalObj.builder();
          }

          @Override
          public Stream<MappingSample> typeVariations(MappingSample base) {
            return Stream.of(
                base,
                //
                new MappingSample(
                    objBuilder().from(base.obj()).credentialRotationRequired(true).build(),
                    entityWithInternalProperty(
                        base.entity(), PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true"))
                //
                );
          }
        });
  }
}
