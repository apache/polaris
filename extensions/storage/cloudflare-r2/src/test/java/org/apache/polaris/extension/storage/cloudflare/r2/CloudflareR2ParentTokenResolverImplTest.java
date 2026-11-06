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
package org.apache.polaris.extension.storage.cloudflare.r2;

import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CloudflareR2ParentTokenResolverImpl}. The values are dummy tokens, not secrets.
 * No test puts a token value in an assertion message: the checks on {@code toString} compare
 * booleans.
 */
class CloudflareR2ParentTokenResolverImplTest {

  private static final CloudflareR2ParentToken DEFAULT_TOKEN =
      new CloudflareR2ParentToken("test-r2-access-key", "test-r2-secret-key");
  private static final CloudflareR2ParentToken TEAM_TOKEN =
      new CloudflareR2ParentToken("test-r2-team-access-key", "test-r2-team-secret-key");

  private static CloudflareR2Configuration configuration(
      Optional<String> accessKey,
      Optional<String> secretKey,
      Map<String, CloudflareR2Configuration.NamedToken> storages) {
    return new CloudflareR2Configuration() {
      @Override
      public Optional<String> accessKey() {
        return accessKey;
      }

      @Override
      public Optional<String> secretKey() {
        return secretKey;
      }

      @Override
      public Map<String, NamedToken> storages() {
        return storages;
      }
    };
  }

  private static CloudflareR2Configuration.NamedToken namedToken(CloudflareR2ParentToken token) {
    return new CloudflareR2Configuration.NamedToken() {
      @Override
      public String accessKey() {
        return token.accessKeyId();
      }

      @Override
      public String secretKey() {
        return token.secretAccessKey();
      }
    };
  }

  private static Optional<String> some(String value) {
    return Optional.of(value);
  }

  @Test
  void defaultPairResolvesForANullStorageName() {
    CloudflareR2ParentTokenResolverImpl resolver =
        new CloudflareR2ParentTokenResolverImpl(
            configuration(
                some(DEFAULT_TOKEN.accessKeyId()),
                some(DEFAULT_TOKEN.secretAccessKey()),
                Map.of()));

    assertThat(resolver.resolve(null)).contains(DEFAULT_TOKEN);
  }

  @Test
  void aHalfSetDefaultPairGivesNoDefaultToken() {
    CloudflareR2ParentTokenResolverImpl accessKeyOnly =
        new CloudflareR2ParentTokenResolverImpl(
            configuration(some(DEFAULT_TOKEN.accessKeyId()), Optional.empty(), Map.of()));
    CloudflareR2ParentTokenResolverImpl secretKeyOnly =
        new CloudflareR2ParentTokenResolverImpl(
            configuration(Optional.empty(), some(DEFAULT_TOKEN.secretAccessKey()), Map.of()));

    assertThat(accessKeyOnly.resolve(null)).isEmpty();
    assertThat(secretKeyOnly.resolve(null)).isEmpty();
  }

  @Test
  void aHalfSetDefaultPairDoesNotHideANamedToken() {
    CloudflareR2ParentTokenResolverImpl resolver =
        new CloudflareR2ParentTokenResolverImpl(
            configuration(
                some(DEFAULT_TOKEN.accessKeyId()),
                Optional.empty(),
                Map.of("team", namedToken(TEAM_TOKEN))));

    assertThat(resolver.resolve(null)).isEmpty();
    assertThat(resolver.resolve("team")).contains(TEAM_TOKEN);
  }

  @Test
  void namedTokenResolvesByItsStorageName() {
    CloudflareR2ParentTokenResolverImpl resolver =
        new CloudflareR2ParentTokenResolverImpl(
            configuration(
                some(DEFAULT_TOKEN.accessKeyId()),
                some(DEFAULT_TOKEN.secretAccessKey()),
                Map.of("team", namedToken(TEAM_TOKEN))));

    assertThat(resolver.resolve("team")).contains(TEAM_TOKEN);
    assertThat(resolver.resolve(null)).contains(DEFAULT_TOKEN);
  }

  @Test
  void unknownStorageNameIsEmptyAndDoesNotFallBackToTheDefaultToken() {
    CloudflareR2ParentTokenResolverImpl resolver =
        new CloudflareR2ParentTokenResolverImpl(
            configuration(
                some(DEFAULT_TOKEN.accessKeyId()),
                some(DEFAULT_TOKEN.secretAccessKey()),
                Map.of("team", namedToken(TEAM_TOKEN))));

    assertThat(resolver.resolve("unknown")).isEmpty();
  }

  @Test
  void noConfigurationResolvesNothing() {
    CloudflareR2ParentTokenResolverImpl resolver =
        new CloudflareR2ParentTokenResolverImpl(
            configuration(Optional.empty(), Optional.empty(), Map.of()));

    assertThat(resolver.resolve(null)).isEmpty();
    assertThat(resolver.resolve("team")).isEmpty();
  }

  @Test
  void laterChangesToTheConfigurationAreNotSeen() {
    Map<String, CloudflareR2Configuration.NamedToken> storages = new HashMap<>();
    storages.put("team", namedToken(TEAM_TOKEN));
    CloudflareR2ParentTokenResolverImpl resolver =
        new CloudflareR2ParentTokenResolverImpl(
            configuration(Optional.empty(), Optional.empty(), storages));

    storages.remove("team");
    storages.put("late", namedToken(DEFAULT_TOKEN));

    assertThat(resolver.resolve("team")).contains(TEAM_TOKEN);
    assertThat(resolver.resolve("late")).isEmpty();
  }

  @Test
  void toStringListsOnlyTheStorageNamesAndWhetherADefaultExists() {
    CloudflareR2ParentTokenResolverImpl resolver =
        new CloudflareR2ParentTokenResolverImpl(
            configuration(
                some(DEFAULT_TOKEN.accessKeyId()),
                some(DEFAULT_TOKEN.secretAccessKey()),
                Map.of("team", namedToken(TEAM_TOKEN), "alpha", namedToken(TEAM_TOKEN))));

    assertThat(resolver.toString())
        .isEqualTo(
            "CloudflareR2ParentTokenResolverImpl{defaultToken=present,"
                + " namedStorages=[alpha, team]}");
    assertThat(new CloudflareR2ParentTokenResolverImpl().toString())
        .isEqualTo("CloudflareR2ParentTokenResolverImpl{defaultToken=absent, namedStorages=[]}");
  }

  @Test
  void toStringContainsNoKeyIdAndNoSecret() {
    CloudflareR2ParentTokenResolverImpl resolver =
        new CloudflareR2ParentTokenResolverImpl(
            configuration(
                some(DEFAULT_TOKEN.accessKeyId()),
                some(DEFAULT_TOKEN.secretAccessKey()),
                Map.of("team", namedToken(TEAM_TOKEN))));
    String text = resolver.toString();

    assertThat(text.contains(DEFAULT_TOKEN.accessKeyId()))
        .as("default key id in toString")
        .isFalse();
    assertThat(text.contains(DEFAULT_TOKEN.secretAccessKey()))
        .as("default secret in toString")
        .isFalse();
    assertThat(text.contains(TEAM_TOKEN.accessKeyId())).as("named key id in toString").isFalse();
    assertThat(text.contains(TEAM_TOKEN.secretAccessKey()))
        .as("named secret in toString")
        .isFalse();
  }

  @Test
  void copiesTheTokensOfARealMapping() {
    CloudflareR2Configuration mapping =
        new SmallRyeConfigBuilder()
            .withMapping(CloudflareR2Configuration.class)
            .withSources(
                new PropertiesConfigSource(
                    Map.of(
                        "polaris.storage.cloudflare-r2.access-key", DEFAULT_TOKEN.accessKeyId(),
                        "polaris.storage.cloudflare-r2.secret-key", DEFAULT_TOKEN.secretAccessKey(),
                        "polaris.storage.cloudflare-r2.team.access-key", TEAM_TOKEN.accessKeyId(),
                        "polaris.storage.cloudflare-r2.team.secret-key",
                            TEAM_TOKEN.secretAccessKey()),
                    "test-properties"))
            .build()
            .getConfigMapping(CloudflareR2Configuration.class);

    CloudflareR2ParentTokenResolverImpl resolver = new CloudflareR2ParentTokenResolverImpl(mapping);

    assertThat(resolver.resolve(null)).contains(DEFAULT_TOKEN);
    assertThat(resolver.resolve("team")).contains(TEAM_TOKEN);
    assertThat(resolver.resolve("unknown")).isEmpty();
  }
}
