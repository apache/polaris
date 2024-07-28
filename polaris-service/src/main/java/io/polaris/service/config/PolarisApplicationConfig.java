/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.core.Configuration;
import io.polaris.core.PolarisConfigurationStore;
import io.polaris.core.auth.AuthenticatedPolarisPrincipal;
import io.polaris.core.persistence.MetaStoreManagerFactory;
import io.polaris.service.auth.DiscoverableAuthenticator;
import io.polaris.service.context.CallContextResolver;
import io.polaris.service.context.RealmContextResolver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration specific to a Polaris REST Service. Place these entries in a YML file for them to
 * be picked up, i.e. `iceberg-rest-server.yml`
 */
public class PolarisApplicationConfig extends Configuration {
  private Map<String, String> sqlLiteCatalogDirs = new HashMap<>();

  private String baseCatalogType;
  private MetaStoreManagerFactory metaStoreManagerFactory;
  private String defaultRealm = "default-realm";
  private RealmContextResolver realmContextResolver;
  private CallContextResolver callContextResolver;
  private DiscoverableAuthenticator<String, AuthenticatedPolarisPrincipal> polarisAuthenticator;
  private CorsConfiguration corsConfiguration = new CorsConfiguration();
  private TaskHandlerConfiguration taskHandler = new TaskHandlerConfiguration();
  private PolarisConfigurationStore configurationStore =
      new DefaultConfigurationStore(new HashMap<>());
  private List<String> defaultRealms;

  public Map<String, String> getSqlLiteCatalogDirs() {
    return sqlLiteCatalogDirs;
  }

  public void setSqlLiteCatalogDirs(Map<String, String> sqlLiteCatalogDirs) {
    this.sqlLiteCatalogDirs = sqlLiteCatalogDirs;
  }

  @JsonProperty("baseCatalogType")
  public void setBaseCatalogType(String baseCatalogType) {
    this.baseCatalogType = baseCatalogType;
  }

  @JsonProperty("baseCatalogType")
  public String getBaseCatalogType() {
    return baseCatalogType;
  }

  @JsonProperty("metaStoreManager")
  public void setMetaStoreManagerFactory(MetaStoreManagerFactory metaStoreManagerFactory) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  @JsonProperty("metaStoreManager")
  public MetaStoreManagerFactory getMetaStoreManagerFactory() {
    return metaStoreManagerFactory;
  }

  @JsonProperty("authenticator")
  public void setPolarisAuthenticator(
      DiscoverableAuthenticator<String, AuthenticatedPolarisPrincipal> polarisAuthenticator) {
    this.polarisAuthenticator = polarisAuthenticator;
  }

  public DiscoverableAuthenticator<String, AuthenticatedPolarisPrincipal>
      getPolarisAuthenticator() {
    return polarisAuthenticator;
  }

  public RealmContextResolver getRealmContextResolver() {
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
  }

  @JsonProperty("cors")
  public CorsConfiguration getCorsConfiguration() {
    return corsConfiguration;
  }

  @JsonProperty("cors")
  public void setCorsConfiguration(CorsConfiguration corsConfiguration) {
    this.corsConfiguration = corsConfiguration;
  }

  public void setTaskHandler(TaskHandlerConfiguration taskHandler) {
    this.taskHandler = taskHandler;
  }

  public TaskHandlerConfiguration getTaskHandler() {
    return taskHandler;
  }

  @JsonProperty("featureConfiguration")
  public void setFeatureConfiguration(Map<String, Object> featureConfiguration) {
    this.configurationStore = new DefaultConfigurationStore(featureConfiguration);
  }

  public PolarisConfigurationStore getConfigurationStore() {
    return configurationStore;
  }

  public List<String> getDefaultRealms() {
    return defaultRealms;
  }

  public void setDefaultRealms(List<String> defaultRealms) {
    this.defaultRealms = defaultRealms;
  }
}
