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
package org.apache.polaris.service.authz;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Startup;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerFactory;
import org.apache.polaris.service.context.RealmContextConfiguration;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.jspecify.annotations.Nullable;

/**
 * Validates the authorizer configuration of every configured realm at startup, by creating each
 * realm's authorizer, so that configuration errors fail startup instead of the realm's requests.
 */
@ApplicationScoped
class AuthorizerValidator {

  private final AuthorizationConfiguration authorizationConfiguration;
  private final RealmContextConfiguration realmContextConfiguration;
  private final Instance<PolarisAuthorizerFactory> factories;
  private final RealmContextHolder realmContextHolder;
  private final ExecutorService executor;

  @Inject
  AuthorizerValidator(
      AuthorizationConfiguration authorizationConfiguration,
      RealmContextConfiguration realmContextConfiguration,
      @Any Instance<PolarisAuthorizerFactory> factories,
      RealmContextHolder realmContextHolder,
      // Note: this executor is expected to NOT propagate CDI contexts to tasks.
      @Identifier("task-executor") ExecutorService executor) {
    this.authorizationConfiguration = authorizationConfiguration;
    this.realmContextConfiguration = realmContextConfiguration;
    this.factories = factories;
    this.realmContextHolder = realmContextHolder;
    this.executor = executor;
  }

  void onStartup(@Observes Startup event) {
    List<String> errors = new ArrayList<>();
    for (String realm : realmContextConfiguration.realms()) {
      validateRealm(realm).ifPresent(errors::add);
    }
    if (!errors.isEmpty()) {
      throw new IllegalStateException(
          "Invalid authorizer configuration:\n  - " + String.join("\n  - ", errors));
    }
  }

  /** Validates the authorizer configuration of the given realm, returning the error, if any. */
  Optional<String> validateRealm(String realm) {
    AuthorizationConfiguration.AuthorizerConfiguration authorizer =
        authorizationConfiguration.authorizer();
    AuthorizationRealmConfiguration realmConfig = authorizer.forRealm(realm);
    String prefix =
        authorizer.realms().containsKey(realm)
            ? "polaris.authorization.authorizer." + realm
            : "polaris.authorization.authorizer";
    if (realmConfig.tenantKey().isPresent() && realmConfig.type().isEmpty()) {
      return Optional.of(
          "realm '%s': %s.tenant-key is set, but %s.type is not".formatted(realm, prefix, prefix));
    }
    String type = realmConfig.effectiveType();
    Instance<PolarisAuthorizerFactory> factory = factories.select(Identifier.Literal.of(type));
    if (factory.isUnsatisfied()) {
      return Optional.of(
          "realm '%s': unknown authorizer type '%s' (%s.type)".formatted(realm, type, prefix));
    }
    Task task =
        new Task(realmContextHolder, realm, factory.get(), realmConfig.tenantKey().orElse(null));
    try {
      // Submit an async task to ensure it runs in a fresh RequestContext.
      executor.submit(task).get(2, TimeUnit.MINUTES);
    } catch (ExecutionException e) {
      return Optional.of(
          "realm '%s': failed to create the '%s' authorizer: %s"
              .formatted(realm, type, e.getCause().getMessage()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
    return Optional.empty();
  }

  private record Task(
      RealmContextHolder realmContextHolder,
      String realmId,
      PolarisAuthorizerFactory factory,
      @Nullable String tenantKey)
      implements Callable<PolarisAuthorizer> {

    @Override
    @ActivateRequestContext
    public PolarisAuthorizer call() {
      // Note: each call to this method runs in a new CDI request context.
      // Make the realm ID effective in the current request context.
      realmContextHolder.set(() -> realmId);
      return factory.create(tenantKey);
    }
  }
}
