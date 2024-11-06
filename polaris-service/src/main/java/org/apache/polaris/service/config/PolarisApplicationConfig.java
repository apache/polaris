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
package org.apache.polaris.service.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Preconditions;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.core.Configuration;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.auth.DecodedToken;
import org.apache.polaris.service.auth.TokenBroker;
import org.apache.polaris.service.auth.TokenBrokerFactory;
import org.apache.polaris.service.auth.TokenResponse;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.context.CallContextResolver;
import org.apache.polaris.service.context.RealmContextResolver;
import org.apache.polaris.service.ratelimiter.RateLimiter;
import org.apache.polaris.service.types.TokenType;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * Configuration specific to a Polaris REST Service. Place these entries in a YML file for them to
 * be picked up, i.e. `iceberg-rest-server.yml`
 */
public class PolarisApplicationConfig extends Configuration {
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
  private TokenBrokerFactory tokenBrokerFactory;

  private AccessToken gcpAccessToken;

  public static final long REQUEST_BODY_BYTES_NO_LIMIT = -1;
  private long maxRequestBodyBytes = REQUEST_BODY_BYTES_NO_LIMIT;

  @JsonProperty("metaStoreManager")
  public void setMetaStoreManagerFactory(MetaStoreManagerFactory metaStoreManagerFactory) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  @JsonProperty("metaStoreManager")
  public MetaStoreManagerFactory getMetaStoreManagerFactory() {
    return metaStoreManagerFactory;
  }

  @JsonProperty("io")
  public void setFileIOFactory(FileIOFactory fileIOFactory) {
    this.fileIOFactory = fileIOFactory;
  }

  @JsonProperty("io")
  public FileIOFactory getFileIOFactory() {
    return fileIOFactory;
  }

  @JsonProperty("authenticator")
  public void setPolarisAuthenticator(
      Authenticator<String, AuthenticatedPolarisPrincipal> polarisAuthenticator) {
    this.polarisAuthenticator = polarisAuthenticator;
  }

  public Authenticator<String, AuthenticatedPolarisPrincipal> getPolarisAuthenticator() {
    return polarisAuthenticator;
  }

  @JsonProperty("tokenBroker")
  public void setTokenBrokerFactory(TokenBrokerFactory tokenBrokerFactory) {
    this.tokenBrokerFactory = tokenBrokerFactory;
  }

  public TokenBrokerFactory getTokenBrokerFactory() {
    // return a no-op implementation if none is specified
    return Objects.requireNonNullElseGet(
        tokenBrokerFactory,
        () ->
            (rc) ->
                new TokenBroker() {
                  @Override
                  public boolean supportsGrantType(String grantType) {
                    return false;
                  }

                  @Override
                  public boolean supportsRequestedTokenType(TokenType tokenType) {
                    return false;
                  }

                  @Override
                  public TokenResponse generateFromClientSecrets(
                      String clientId, String clientSecret, String grantType, String scope) {
                    return null;
                  }

                  @Override
                  public TokenResponse generateFromToken(
                      TokenType tokenType, String subjectToken, String grantType, String scope) {
                    return null;
                  }

                  @Override
                  public DecodedToken verify(String token) {
                    return null;
                  }
                });
  }

  public RealmContextResolver getRealmContextResolver() {
    realmContextResolver.setDefaultRealm(this.defaultRealm);
    return realmContextResolver;
  }

  public void setRealmContextResolver(RealmContextResolver realmContextResolver) {
    this.realmContextResolver = realmContextResolver;
  }

  public CallContextResolver getCallContextResolver() {
    return callContextResolver;
  }

  @JsonProperty("callContextResolver")
  public void setCallContextResolver(CallContextResolver callContextResolver) {
    this.callContextResolver = callContextResolver;
  }

  private OAuth2ApiService oauth2Service;

  @JsonProperty("oauth2")
  public void setOauth2Service(OAuth2ApiService oauth2Service) {
    this.oauth2Service = oauth2Service;
  }

  public OAuth2ApiService getOauth2Service() {
    return oauth2Service;
  }

  public String getDefaultRealm() {
    return defaultRealm;
  }

  @JsonProperty("defaultRealm")
  public void setDefaultRealm(String defaultRealm) {
    this.defaultRealm = defaultRealm;
    realmContextResolver.setDefaultRealm(defaultRealm);
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
  public RateLimiter getRateLimiter() {
    return rateLimiter;
  }

  @JsonProperty("rateLimiter")
  public void setRateLimiter(@Nullable RateLimiter rateLimiter) {
    this.rateLimiter = rateLimiter;
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

  public PolarisConfigurationStore getConfigurationStore() {
    return new DefaultConfigurationStore(globalFeatureConfiguration, realmConfiguration);
  }

  public List<String> getDefaultRealms() {
    return defaultRealms;
  }

  public AwsCredentialsProvider credentialsProvider() {
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

  public Supplier<GoogleCredentials> getGcpCredentialsProvider() {
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
