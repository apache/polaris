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
package org.apache.polaris.service.quarkus.config;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.context.SmallRyeManagedExecutor;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.Context;
import java.time.Clock;
import java.util.HashMap;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.context.RealmId;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.auth.Authenticator;
import org.apache.polaris.service.auth.TokenBroker;
import org.apache.polaris.service.auth.TokenBrokerFactory;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2ApiService;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.context.RealmContextConfiguration;
import org.apache.polaris.service.context.RealmIdResolver;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.quarkus.auth.QuarkusAuthenticationConfiguration;
import org.apache.polaris.service.quarkus.catalog.io.QuarkusFileIOConfiguration;
import org.apache.polaris.service.quarkus.context.QuarkusRealmContextConfiguration;
import org.apache.polaris.service.quarkus.persistence.QuarkusPersistenceConfiguration;
import org.apache.polaris.service.quarkus.ratelimiter.QuarkusRateLimiterFilterConfiguration;
import org.apache.polaris.service.quarkus.ratelimiter.QuarkusTokenBucketConfiguration;
import org.apache.polaris.service.ratelimiter.RateLimiter;
import org.apache.polaris.service.ratelimiter.TokenBucketFactory;
import org.apache.polaris.service.task.TaskHandlerConfiguration;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.eclipse.microprofile.context.ThreadContext;

public class QuarkusProducers {

  @Produces
  @ApplicationScoped // cannot be singleton because it is mocked in tests
  public Clock clock() {
    return Clock.systemUTC();
  }

  // Polaris core beans - application scope

  @Produces
  @ApplicationScoped
  public StorageCredentialCache storageCredentialCache(
      PolarisDiagnostics diagnostics, PolarisConfigurationStore configurationStore) {
    return new StorageCredentialCache(diagnostics, configurationStore);
  }

  @Produces
  @ApplicationScoped
  public PolarisAuthorizer polarisAuthorizer(PolarisConfigurationStore configurationStore) {
    return new PolarisAuthorizerImpl(configurationStore);
  }

  @Produces
  @ApplicationScoped
  public PolarisDiagnostics polarisDiagnostics() {
    return new PolarisDefaultDiagServiceImpl();
  }

  // Polaris core beans - request scope

  @Produces
  @RequestScoped
  public RealmId realmId(@Context HttpServerRequest request, RealmIdResolver realmIdResolver) {
    return realmIdResolver.resolveRealmContext(
        request.absoluteURI(),
        request.method().name(),
        request.path(),
        request.headers().entries().stream()
            .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll));
  }

  @Produces
  @RequestScoped
  public PolarisMetaStoreSession metaStoreSession(
      MetaStoreManagerFactory metaStoreManagerFactory, RealmId realmId) {
    return metaStoreManagerFactory.getOrCreateSessionSupplier(realmId).get();
  }

  @Produces
  @RequestScoped
  // TODO break into separate beans
  public PolarisMetaStoreManager polarisMetaStoreManager(
      MetaStoreManagerFactory metaStoreManagerFactory, RealmId realmId) {
    return metaStoreManagerFactory.getOrCreateMetaStoreManager(realmId);
  }

  @Produces
  @RequestScoped
  public StorageCredentialCache storageCredentialCache(
      MetaStoreManagerFactory metaStoreManagerFactory, RealmId realmId) {
    return metaStoreManagerFactory.getOrCreateStorageCredentialCache(realmId);
  }

  @Produces
  @RequestScoped
  public EntityCache entityCache(MetaStoreManagerFactory metaStoreManagerFactory, RealmId realmId) {
    return metaStoreManagerFactory.getOrCreateEntityCache(realmId);
  }

  @Produces
  @RequestScoped
  public PolarisEntityManager polarisEntityManager(
      RealmEntityManagerFactory realmEntityManagerFactory, RealmId realmId) {
    return realmEntityManagerFactory.getOrCreateEntityManager(realmId);
  }

  @Produces
  @RequestScoped
  public TokenBroker tokenBroker(TokenBrokerFactory tokenBrokerFactory, RealmId realmId) {
    return tokenBrokerFactory.apply(realmId);
  }

  // Polaris service beans - selected from @Identifier-annotated beans

  @Produces
  public RealmIdResolver realmContextResolver(
      QuarkusRealmContextConfiguration config,
      @Any Instance<RealmIdResolver> realmContextResolvers) {
    return realmContextResolvers.select(Identifier.Literal.of(config.type())).get();
  }

  @Produces
  public FileIOFactory fileIOFactory(
          QuarkusFileIOConfiguration config, @Any Instance<FileIOFactory> fileIOFactories) {
    return fileIOFactories.select(Identifier.Literal.of(config.type())).get();
  }

  @Produces
  public MetaStoreManagerFactory metaStoreManagerFactory(
      QuarkusPersistenceConfiguration config,
      @Any Instance<MetaStoreManagerFactory> metaStoreManagerFactories) {
    return metaStoreManagerFactories.select(Identifier.Literal.of(config.type())).get();
  }

  /**
   * Eagerly initialize the in-memory default realm on startup, so that users can check the
   * credentials printed to stdout immediately.
   */
  public void maybeInitializeInMemoryRealm(
      @Observes StartupEvent event,
      MetaStoreManagerFactory factory,
      RealmContextConfiguration realmContextConfiguration) {
    if (factory instanceof InMemoryPolarisMetaStoreManagerFactory) {
      ((InMemoryPolarisMetaStoreManagerFactory) factory).onStartup(realmContextConfiguration);
    }
  }

  @Produces
  public RateLimiter rateLimiter(
      QuarkusRateLimiterFilterConfiguration config, @Any Instance<RateLimiter> rateLimiters) {
    return rateLimiters.select(Identifier.Literal.of(config.type())).get();
  }

  @Produces
  public TokenBucketFactory tokenBucketFactory(
      QuarkusTokenBucketConfiguration config,
      @Any Instance<TokenBucketFactory> tokenBucketFactories) {
    return tokenBucketFactories.select(Identifier.Literal.of(config.type())).get();
  }

  @Produces
  public Authenticator<String, AuthenticatedPolarisPrincipal> authenticator(
      QuarkusAuthenticationConfiguration config,
      @Any Instance<Authenticator<String, AuthenticatedPolarisPrincipal>> authenticators) {
    return authenticators.select(Identifier.Literal.of(config.authenticator().type())).get();
  }

  @Produces
  public IcebergRestOAuth2ApiService icebergRestOAuth2ApiService(
      QuarkusAuthenticationConfiguration config,
      @Any Instance<IcebergRestOAuth2ApiService> services) {
    return services.select(Identifier.Literal.of(config.tokenService().type())).get();
  }

  @Produces
  public TokenBrokerFactory tokenBrokerFactory(
      QuarkusAuthenticationConfiguration config,
      @Any Instance<TokenBrokerFactory> tokenBrokerFactories) {
    return tokenBrokerFactories.select(Identifier.Literal.of(config.tokenBroker().type())).get();
  }

  // other beans

  @Produces
  @Singleton
  @Identifier("task-executor")
  public ManagedExecutor taskExecutor(TaskHandlerConfiguration config) {
    return SmallRyeManagedExecutor.builder()
        .injectionPointName("task-executor")
        .propagated(ThreadContext.ALL_REMAINING)
        .maxAsync(config.maxConcurrentTasks())
        .maxQueued(config.maxQueuedTasks())
        .build();
  }

  public void closeTaskExecutor(@Disposes @Identifier("task-executor") ManagedExecutor executor) {
    executor.close();
  }
}
