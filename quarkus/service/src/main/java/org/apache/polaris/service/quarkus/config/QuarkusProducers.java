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

import io.smallrye.common.annotation.Identifier;
import io.smallrye.context.SmallRyeManagedExecutor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Startup;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import java.time.Clock;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.secrets.UserSecretsManagerFactory;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.auth.ActiveRolesProvider;
import org.apache.polaris.service.auth.AuthenticationType;
import org.apache.polaris.service.auth.Authenticator;
import org.apache.polaris.service.auth.PrincipalCredential;
import org.apache.polaris.service.auth.TokenBroker;
import org.apache.polaris.service.auth.TokenBrokerFactory;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2ApiService;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.context.RealmContextConfiguration;
import org.apache.polaris.service.context.RealmContextFilter;
import org.apache.polaris.service.context.RealmContextResolver;
import org.apache.polaris.service.quarkus.auth.QuarkusAuthenticationConfiguration;
import org.apache.polaris.service.quarkus.auth.QuarkusAuthenticationRealmConfiguration;
import org.apache.polaris.service.quarkus.auth.external.tenant.OidcTenantResolver;
import org.apache.polaris.service.quarkus.catalog.io.QuarkusFileIOConfiguration;
import org.apache.polaris.service.quarkus.context.QuarkusRealmContextConfiguration;
import org.apache.polaris.service.quarkus.persistence.QuarkusPersistenceConfiguration;
import org.apache.polaris.service.quarkus.ratelimiter.QuarkusRateLimiterFilterConfiguration;
import org.apache.polaris.service.quarkus.ratelimiter.QuarkusTokenBucketConfiguration;
import org.apache.polaris.service.quarkus.secrets.QuarkusSecretsManagerConfiguration;
import org.apache.polaris.service.ratelimiter.RateLimiter;
import org.apache.polaris.service.ratelimiter.TokenBucketFactory;
import org.apache.polaris.service.task.TaskHandlerConfiguration;
import org.eclipse.microprofile.config.inject.ConfigProperty;
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
  public StorageCredentialCache storageCredentialCache() {
    return new StorageCredentialCache();
  }

  @Produces
  @ApplicationScoped
  public PolarisAuthorizer polarisAuthorizer(PolarisConfigurationStore configurationStore) {
    return new PolarisAuthorizerImpl(configurationStore);
  }

  @Produces
  @Singleton
  public PolarisDiagnostics polarisDiagnostics() {
    return new PolarisDefaultDiagServiceImpl();
  }

  // Polaris core beans - request scope

  @Produces
  @RequestScoped
  public RealmContext realmContext(@Context ContainerRequestContext request) {
    return (RealmContext) request.getProperty(RealmContextFilter.REALM_CONTEXT_KEY);
  }

  @Produces
  @RequestScoped
  public PolarisCallContext polarisCallContext(
      RealmContext realmContext,
      PolarisDiagnostics diagServices,
      PolarisConfigurationStore configurationStore,
      MetaStoreManagerFactory metaStoreManagerFactory,
      Clock clock) {
    BasePersistence metaStoreSession =
        metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get();
    return new PolarisCallContext(metaStoreSession, diagServices, configurationStore, clock);
  }

  @Produces
  @RequestScoped
  public CallContext callContext(RealmContext realmContext, PolarisCallContext polarisCallContext) {
    return CallContext.of(realmContext, polarisCallContext);
  }

  public void closeCallContext(@Disposes CallContext callContext) {
    callContext.close();
  }

  // Polaris service beans - selected from @Identifier-annotated beans

  @Produces
  public RealmContextResolver realmContextResolver(
      QuarkusRealmContextConfiguration config,
      @Any Instance<RealmContextResolver> realmContextResolvers) {
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

  @Produces
  public UserSecretsManagerFactory userSecretsManagerFactory(
      QuarkusSecretsManagerConfiguration config,
      @Any Instance<UserSecretsManagerFactory> userSecretsManagerFactories) {
    return userSecretsManagerFactories.select(Identifier.Literal.of(config.type())).get();
  }

  /**
   * Eagerly initialize the in-memory default realm on startup, so that users can check the
   * credentials printed to stdout immediately.
   */
  public void maybeBootstrap(
      @Observes Startup event,
      MetaStoreManagerFactory factory,
      QuarkusPersistenceConfiguration config,
      RealmContextConfiguration realmContextConfiguration) {
    if (config.isAutoBootstrap()) {
      RootCredentialsSet rootCredentialsSet = RootCredentialsSet.fromEnvironment();
      factory.bootstrapRealms(realmContextConfiguration.realms(), rootCredentialsSet);
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
  @RequestScoped
  public Authenticator<PrincipalCredential, AuthenticatedPolarisPrincipal> authenticator(
      QuarkusAuthenticationRealmConfiguration config,
      @Any
          Instance<Authenticator<PrincipalCredential, AuthenticatedPolarisPrincipal>>
              authenticators) {
    return authenticators.select(Identifier.Literal.of(config.authenticator().type())).get();
  }

  @Produces
  @RequestScoped
  public IcebergRestOAuth2ApiService icebergRestOAuth2ApiService(
      QuarkusAuthenticationRealmConfiguration config,
      @Any Instance<IcebergRestOAuth2ApiService> services) {
    String type =
        config.type() == AuthenticationType.EXTERNAL ? "disabled" : config.tokenService().type();
    return services.select(Identifier.Literal.of(type)).get();
  }

  @Produces
  @RequestScoped
  public TokenBroker tokenBroker(
      QuarkusAuthenticationRealmConfiguration config,
      RealmContext realmContext,
      @Any Instance<TokenBrokerFactory> tokenBrokerFactories) {
    String type =
        config.type() == AuthenticationType.EXTERNAL ? "none" : config.tokenBroker().type();
    TokenBrokerFactory tokenBrokerFactory =
        tokenBrokerFactories.select(Identifier.Literal.of(type)).get();
    return tokenBrokerFactory.apply(realmContext);
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

  @Produces
  @RequestScoped
  public PolarisMetaStoreManager polarisMetaStoreManager(
      RealmContext realmContext, MetaStoreManagerFactory metaStoreManagerFactory) {
    return metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
  }

  @Produces
  @RequestScoped
  public UserSecretsManager userSecretsManager(
      RealmContext realmContext, UserSecretsManagerFactory userSecretsManagerFactory) {
    return userSecretsManagerFactory.getOrCreateUserSecretsManager(realmContext);
  }

  @Produces
  @RequestScoped
  public BasePersistence polarisMetaStoreSession(
      RealmContext realmContext, MetaStoreManagerFactory metaStoreManagerFactory) {
    return metaStoreManagerFactory.getOrCreateSessionSupplier(realmContext).get();
  }

  @Produces
  @RequestScoped
  public PolarisEntityManager polarisEntityManager(
      RealmContext realmContext, RealmEntityManagerFactory factory) {
    return factory.getOrCreateEntityManager(realmContext);
  }

  @Produces
  @RequestScoped
  public QuarkusAuthenticationRealmConfiguration realmAuthConfig(
      QuarkusAuthenticationConfiguration config, RealmContext realmContext) {
    return config.forRealm(realmContext);
  }

  @Produces
  public ActiveRolesProvider activeRolesProvider(
      @ConfigProperty(name = "polaris.active-roles-provider.type") String activeRolesProviderType,
      @Any Instance<ActiveRolesProvider> activeRolesProviders) {
    return activeRolesProviders.select(Identifier.Literal.of(activeRolesProviderType)).get();
  }

  @Produces
  public OidcTenantResolver oidcTenantResolver(
      org.apache.polaris.service.quarkus.auth.external.OidcConfiguration config,
      @Any Instance<OidcTenantResolver> resolvers) {
    return resolvers.select(Identifier.Literal.of(config.tenantResolver())).get();
  }

  public void closeTaskExecutor(@Disposes @Identifier("task-executor") ManagedExecutor executor) {
    executor.close();
  }
}
