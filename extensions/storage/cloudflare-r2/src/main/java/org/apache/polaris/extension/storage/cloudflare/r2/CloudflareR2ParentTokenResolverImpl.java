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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import org.jspecify.annotations.Nullable;

/**
 * CDI adapter that exposes {@link CloudflareR2Configuration} as an {@link
 * CloudflareR2ParentTokenResolver}. The constructor copies the default token and every named token,
 * so the bean holds no reference to the configuration mapping. An entry of the default token counts
 * as present only when both keys are set. A catalog without a {@code storageName} resolves to the
 * default token. A catalog with a {@code storageName} resolves to the named token. The mechanism
 * decides which of the two applies (see {@link
 * CloudflareR2ParentTokenResolver#effectiveStorageName}).
 */
@ApplicationScoped
public class CloudflareR2ParentTokenResolverImpl implements CloudflareR2ParentTokenResolver {

  private final Optional<CloudflareR2ParentToken> defaultToken;
  private final Map<String, CloudflareR2ParentToken> namedTokens;

  /**
   * For the client proxy ArC generates for this normal-scoped bean: the proxy subclass needs a
   * no-args constructor. A working instance always comes from the constructor below.
   */
  protected CloudflareR2ParentTokenResolverImpl() {
    this.defaultToken = Optional.empty();
    this.namedTokens = Map.of();
  }

  @Inject
  public CloudflareR2ParentTokenResolverImpl(CloudflareR2Configuration configuration) {
    Optional<String> accessKey = configuration.accessKey();
    Optional<String> secretKey = configuration.secretKey();
    this.defaultToken =
        accessKey.isPresent() && secretKey.isPresent()
            ? Optional.of(new CloudflareR2ParentToken(accessKey.get(), secretKey.get()))
            : Optional.empty();
    Map<String, CloudflareR2ParentToken> named = new HashMap<>();
    configuration
        .storages()
        .forEach(
            (storageName, token) ->
                named.put(
                    storageName,
                    new CloudflareR2ParentToken(token.accessKey(), token.secretKey())));
    this.namedTokens = Map.copyOf(named);
  }

  @Override
  public Optional<CloudflareR2ParentToken> resolve(@Nullable String storageName) {
    if (storageName == null) {
      return defaultToken;
    }
    return Optional.ofNullable(namedTokens.get(storageName));
  }

  /** Lists the configured storage names and whether a default token exists. No key, no secret. */
  @Override
  public String toString() {
    return "CloudflareR2ParentTokenResolverImpl{defaultToken="
        + (defaultToken.isPresent() ? "present" : "absent")
        + ", namedStorages="
        + new TreeSet<>(namedTokens.keySet())
        + "}";
  }
}
