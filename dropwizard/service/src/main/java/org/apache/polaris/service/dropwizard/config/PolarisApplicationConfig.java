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
package org.apache.polaris.service.dropwizard.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Preconditions;
import io.dropwizard.core.Configuration;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.service.auth.Authenticator;
import org.apache.polaris.service.auth.TokenBrokerFactory;
import org.apache.polaris.service.auth.TokenBrokerFactoryConfig;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2ApiService;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.config.DefaultConfigurationStore;
import org.apache.polaris.service.config.TaskHandlerConfiguration;
import org.apache.polaris.service.context.CallContextResolver;
import org.apache.polaris.service.context.RealmContextResolver;
import org.apache.polaris.service.ratelimiter.RateLimiter;
import org.apache.polaris.service.ratelimiter.TokenBucketFactory;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

/**
 * Configuration specific to a Polaris REST Service. Place these entries in a YML file for them to
 * be picked up, i.e. `iceberg-rest-server.yml`
 */
public class PolarisApplicationConfig extends Configuration {
  /**
   * Override the default binding of registered services so that the configured instances are used.
   */
  private static final int OVERRIDE_BINDING_RANK = 10;

  private MetaStoreManagerFactory metaStoreManagerFactory;
  private String defaultRealm = "default-realm";
  private RealmContextResolver realmContextResolver;
  private CallContextResolver callContextResolver;
  private Authenticator<String, AuthenticatedPolarisPrincipal> polarisAuthenticator;
  private CorsConfiguration corsConfiguration = new CorsConfiguration();
  private TaskHandlerConfiguration taskHandler = new TaskHandlerConfiguration();
  private Map<String, Object> globalFeatureConfiguration = Map.of();
  private Map<String, Map<String, Object>> realmConfiguration = Map.of();
  private List<String> defaultRealms;
  private String awsAccessKey;
  private String awsSecretKey;
  private FileIOFactory fileIOFactory;
  private RateLimiter rateLimiter;
  private TokenBucketFactory tokenBucketFactory;
  private TokenBrokerConfig tokenBroker = new TokenBrokerConfig();

  private AccessToken gcpAccessToken;

  public static final long REQUEST_BODY_BYTES_NO_LIMIT = -1;
  private long maxRequestBodyBytes = REQUEST_BODY_BYTES_NO_LIMIT;

  @Inject ServiceLocator serviceLocator;

  @PostConstruct
  public void bindToServiceLocator() {
    ServiceLocatorUtilities.bind(serviceLocator, binder());
  }

  public ServiceLocator getServiceLocator() {
    return serviceLocator;
  }

  @Nonnull
  public AbstractBinder binder() {
    PolarisApplicationConfig config = this;
    return new AbstractBinder() {
      @Override
      protected void configure() {
        bindFactory(SupplierFactory.create(serviceLocator, config::getStorageIntegrationProvider))
            .to(PolarisStorageIntegrationProvider.class)
            .ranked(OVERRIDE_BINDING_RANK);
        bindFactory(SupplierFactory.create(serviceLocator, config::getMetaStoreManagerFactory))
            .to(MetaStoreManagerFactory.class)
            .ranked(OVERRIDE_BINDING_RANK);
        bindFactory(SupplierFactory.create(serviceLocator, config::createConfigurationStore))
            .to(PolarisConfigurationStore.class)
            .ranked(OVERRIDE_BINDING_RANK);
        bindFactory(SupplierFactory.create(serviceLocator, config::getFileIOFactory))
            .to(FileIOFactory.class)
            .ranked(OVERRIDE_BINDING_RANK);
        bindFactory(SupplierFactory.create(serviceLocator, config::getPolarisAuthenticator))
            .to(Authenticator.class)
            .ranked(OVERRIDE_BINDING_RANK);
        bindFactory(SupplierFactory.create(serviceLocator, () -> tokenBroker))
            .to(TokenBrokerFactoryConfig.class);
        bindFactory(
                SupplierFactory.create(
                    serviceLocator,
                    () ->
                        serviceLocator.getService(TokenBrokerFactory.class, tokenBroker.getType())))
            .to(TokenBrokerFactory.class)
            .ranked(OVERRIDE_BINDING_RANK);
        bindFactory(SupplierFactory.create(serviceLocator, config::getOauth2Service))
            .to(IcebergRestOAuth2ApiService.class)
            .ranked(OVERRIDE_BINDING_RANK);
        bindFactory(SupplierFactory.create(serviceLocator, config::getCallContextResolver))
            .to(CallContextResolver.class)
            .ranked(OVERRIDE_BINDING_RANK);
        bindFactory(SupplierFactory.create(serviceLocator, config::getRealmContextResolver))
            .to(RealmContextResolver.class)
            .ranked(OVERRIDE_BINDING_RANK);
        bindFactory(SupplierFactory.create(serviceLocator, config::getRateLimiter))
            .to(RateLimiter.class)
            .ranked(OVERRIDE_BINDING_RANK);
        bindFactory(SupplierFactory.create(serviceLocator, config::getTokenBucketFactory))
            .to(TokenBucketFactory.class)
            .ranked(OVERRIDE_BINDING_RANK);
      }
    };
  }

  /**
   * Factory implementation that uses the provided supplier method to retrieve the instance and then
   * uses the {@link #serviceLocator} to inject dependencies into the instance. This is necessary
   * since the DI framework doesn't automatically inject dependencies into the instances created.
   *
   * @param <T>
   */
  private static final class SupplierFactory<T> implements Factory<T> {
    private final ServiceLocator serviceLocator;
    private final Supplier<T> supplier;

    private static <T> SupplierFactory<T> create(
        ServiceLocator serviceLocator, Supplier<T> supplier) {
      return new SupplierFactory<>(serviceLocator, supplier);
    }

    private SupplierFactory(ServiceLocator serviceLocator, Supplier<T> supplier) {
      this.serviceLocator = serviceLocator;
      this.supplier = supplier;
    }

    @Override
    public T provide() {
      T obj = supplier.get();
      serviceLocator.inject(obj);
      return obj;
    }

    @Override
    public void dispose(T instance) {}
  }

  public <T> T findService(Class<T> serviceClass) {
    return serviceLocator.getService(serviceClass);
  }

  public <T> T findService(TypeLiteral<T> serviceClass) {
    return serviceLocator.getService(serviceClass.getRawType());
  }

  @JsonProperty("metaStoreManager")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  public void setMetaStoreManagerFactory(MetaStoreManagerFactory metaStoreManagerFactory) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  private MetaStoreManagerFactory getMetaStoreManagerFactory() {
    return metaStoreManagerFactory;
  }

  @JsonProperty("io")
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      include = JsonTypeInfo.As.PROPERTY,
      property = "factoryType")
  public void setFileIOFactory(FileIOFactory fileIOFactory) {
    this.fileIOFactory = fileIOFactory;
  }

  private FileIOFactory getFileIOFactory() {
    return fileIOFactory;
  }

  @JsonProperty("authenticator")
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
  public void setPolarisAuthenticator(
      Authenticator<String, AuthenticatedPolarisPrincipal> polarisAuthenticator) {
    this.polarisAuthenticator = polarisAuthenticator;
  }

  private Authenticator<String, AuthenticatedPolarisPrincipal> getPolarisAuthenticator() {
    return polarisAuthenticator;
  }

  @JsonProperty("tokenBroker")
  public void setTokenBroker(TokenBrokerConfig tokenBroker) {
    this.tokenBroker = tokenBroker;
  }

  private RealmContextResolver getRealmContextResolver() {
    realmContextResolver.setDefaultRealm(this.defaultRealm);
    return realmContextResolver;
  }

  @JsonProperty("realmContextResolver")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  public void setRealmContextResolver(RealmContextResolver realmContextResolver) {
    this.realmContextResolver = realmContextResolver;
  }

  private CallContextResolver getCallContextResolver() {
    return callContextResolver;
  }

  @JsonProperty("callContextResolver")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  public void setCallContextResolver(CallContextResolver callContextResolver) {
    this.callContextResolver = callContextResolver;
  }

  private IcebergRestOAuth2ApiService oauth2Service;

  @JsonProperty("oauth2")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  public void setOauth2Service(IcebergRestOAuth2ApiService oauth2Service) {
    this.oauth2Service = oauth2Service;
  }

  private IcebergRestOAuth2ApiService getOauth2Service() {
    return oauth2Service;
  }

  public String getDefaultRealm() {
    return defaultRealm;
  }

  @JsonProperty("defaultRealm")
  public void setDefaultRealm(String defaultRealm) {
    this.defaultRealm = defaultRealm;
  }

  @JsonProperty("cors")
  public CorsConfiguration getCorsConfiguration() {
    return corsConfiguration;
  }

  @JsonProperty("cors")
  public void setCorsConfiguration(CorsConfiguration corsConfiguration) {
    this.corsConfiguration = corsConfiguration;
  }

  @JsonProperty("rateLimiter")
  private RateLimiter getRateLimiter() {
    return rateLimiter;
  }

  public boolean hasRateLimiter() {
    return rateLimiter != null;
  }

  @JsonProperty("rateLimiter")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  public void setRateLimiter(@Nullable RateLimiter rateLimiter) {
    this.rateLimiter = rateLimiter;
  }

  @JsonProperty("tokenBucketFactory")
  private TokenBucketFactory getTokenBucketFactory() {
    return tokenBucketFactory;
  }

  @JsonProperty("tokenBucketFactory")
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  public void setTokenBucketFactory(@Nullable TokenBucketFactory tokenBucketFactory) {
    this.tokenBucketFactory = tokenBucketFactory;
  }

  public void setTaskHandler(TaskHandlerConfiguration taskHandler) {
    this.taskHandler = taskHandler;
  }

  public TaskHandlerConfiguration getTaskHandler() {
    return taskHandler;
  }

  @JsonProperty("featureConfiguration")
  public void setFeatureConfiguration(Map<String, Object> featureConfiguration) {
    this.globalFeatureConfiguration = featureConfiguration;
  }

  @JsonProperty("realmFeatureConfiguration")
  public void setRealmFeatureConfiguration(Map<String, Map<String, Object>> realmConfiguration) {
    this.realmConfiguration = realmConfiguration;
  }

  @JsonProperty("maxRequestBodyBytes")
  public void setMaxRequestBodyBytes(long maxRequestBodyBytes) {
    // The underlying library that we use to implement the limit treats all values <= 0 as the
    // same, so we block all but -1 to prevent ambiguity.
    Preconditions.checkArgument(
        maxRequestBodyBytes == -1 || maxRequestBodyBytes > 0,
        "maxRequestBodyBytes must be a positive integer or %s to specify no limit.",
        REQUEST_BODY_BYTES_NO_LIMIT);

    this.maxRequestBodyBytes = maxRequestBodyBytes;
  }

  public long getMaxRequestBodyBytes() {
    return maxRequestBodyBytes;
  }

  private PolarisConfigurationStore createConfigurationStore() {
    return new DefaultConfigurationStore(globalFeatureConfiguration, realmConfiguration);
  }

  public List<String> getDefaultRealms() {
    return defaultRealms;
  }

  private AwsCredentialsProvider credentialsProvider() {
    if (StringUtils.isNotBlank(awsAccessKey) && StringUtils.isNotBlank(awsSecretKey)) {
      LoggerFactory.getLogger(PolarisApplicationConfig.class)
          .warn("Using hard-coded AWS credentials - this is not recommended for production");
      return StaticCredentialsProvider.create(
          AwsBasicCredentials.create(awsAccessKey, awsSecretKey));
    }
    return null;
  }

  public void setAwsAccessKey(String awsAccessKey) {
    this.awsAccessKey = awsAccessKey;
  }

  public void setAwsSecretKey(String awsSecretKey) {
    this.awsSecretKey = awsSecretKey;
  }

  public void setDefaultRealms(List<String> defaultRealms) {
    this.defaultRealms = defaultRealms;
  }

  private PolarisStorageIntegrationProvider storageIntegrationProvider;

  public void setStorageIntegrationProvider(
      PolarisStorageIntegrationProvider storageIntegrationProvider) {
    this.storageIntegrationProvider = storageIntegrationProvider;
  }

  private PolarisStorageIntegrationProvider getStorageIntegrationProvider() {
    if (storageIntegrationProvider == null) {
      storageIntegrationProvider =
          new PolarisStorageIntegrationProviderImpl(
              () -> {
                StsClientBuilder stsClientBuilder = StsClient.builder();
                AwsCredentialsProvider awsCredentialsProvider = credentialsProvider();
                if (awsCredentialsProvider != null) {
                  stsClientBuilder.credentialsProvider(awsCredentialsProvider);
                }
                return stsClientBuilder.build();
              },
              getGcpCredentialsProvider());
    }
    return storageIntegrationProvider;
  }

  private Supplier<GoogleCredentials> getGcpCredentialsProvider() {
    return () ->
        Optional.ofNullable(gcpAccessToken)
            .map(GoogleCredentials::create)
            .orElseGet(
                () -> {
                  try {
                    return GoogleCredentials.getApplicationDefault();
                  } catch (IOException e) {
                    throw new RuntimeException("Failed to get GCP credentials", e);
                  }
                });
  }

  @JsonProperty("gcp_credentials")
  void setGcpCredentials(GcpAccessToken token) {
    this.gcpAccessToken =
        new AccessToken(
            token.getAccessToken(),
            new Date(System.currentTimeMillis() + token.getExpiresIn() * 1000));
  }

  /**
   * A static AccessToken representation used to store a static token and expiration date. This
   * should strictly be used for testing.
   */
  static class GcpAccessToken {
    private String accessToken;
    private long expiresIn;

    public GcpAccessToken() {}

    public GcpAccessToken(String accessToken, long expiresIn) {
      this.accessToken = accessToken;
      this.expiresIn = expiresIn;
    }

    public String getAccessToken() {
      return accessToken;
    }

    @JsonProperty("access_token")
    public void setAccessToken(String accessToken) {
      this.accessToken = accessToken;
    }

    public long getExpiresIn() {
      return expiresIn;
    }

    @JsonProperty("expires_in")
    public void setExpiresIn(long expiresIn) {
      this.expiresIn = expiresIn;
    }
  }
}
