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
package org.apache.polaris.service.admin;

import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.core.setup.Environment;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Feature;
import jakarta.ws.rs.core.FeatureContext;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.CreateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.cache.EntityCacheByNameKey;
import org.apache.polaris.core.persistence.cache.EntityCacheEntry;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.apache.polaris.service.test.TestEnvironmentExtension;
import org.glassfish.hk2.api.ServiceLocator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * tests around the {@link org.apache.polaris.service.persistence.cache.EntityCacheFactory} and
 * ensuring that the {@link org.apache.polaris.core.persistence.cache.EntityCache} is managed per
 * realm.
 */
@ExtendWith({
  DropwizardExtensionsSupport.class,
  TestEnvironmentExtension.class,
  PolarisConnectionExtension.class
})
public class PolarisRealmEntityCacheTest {
  private static ServiceLocatorAccessor accessor = new ServiceLocatorAccessor();

  /**
   * Injectable {@link Feature} that allows us to access the {@link ServiceLocator} from the jersey
   * resource configuration.
   */
  private static final class ServiceLocatorAccessor implements Feature {
    @Inject ServiceLocator serviceLocator;

    @Override
    public boolean configure(FeatureContext context) {
      return true;
    }

    public ServiceLocator getServiceLocator() {
      return serviceLocator;
    }
  }

  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
              PolarisApplication.class,
              ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
              ConfigOverride.config(
                  "server.applicationConnectors[0].port",
                  "0"), // Bind to random port to support parallelism
              ConfigOverride.config("server.adminConnectors[0].port", "0"))
          .addListener(
              new DropwizardAppExtension.ServiceListener<PolarisApplicationConfig>() {
                @Override
                public void onRun(
                    PolarisApplicationConfig configuration,
                    Environment environment,
                    DropwizardAppExtension<PolarisApplicationConfig> rule)
                    throws Exception {
                  environment.jersey().register(accessor);
                }
              });
  private static String userToken;
  private static String realm;

  @BeforeAll
  public static void setup(
      PolarisConnectionExtension.PolarisToken adminToken, @PolarisRealm String polarisRealm)
      throws IOException {
    userToken = adminToken.token();
    realm = polarisRealm;

    // Set up test location
    PolarisConnectionExtension.createTestDir(realm);
  }

  @Test
  public void testRealmEntityCacheEquality() {
    ServiceLocator serviceLocator = accessor.getServiceLocator();
    EntityCache cache1;
    // check that multiple calls to the serviceLocator return the same instance of the EntityCache
    // within the same call context
    try (CallContext ctx =
        CallContext.setCurrentContext(
            CallContext.of(() -> realm, new PolarisCallContext(null, null)))) {
      cache1 = serviceLocator.getService(EntityCache.class);
      EntityCache cache2 = serviceLocator.getService(EntityCache.class);
      assertThat(cache1).isSameAs(cache2);
    }

    // in a new call context with a different realm, the EntityCache should be different
    try (CallContext ctx =
        CallContext.setCurrentContext(
            CallContext.of(() -> "anotherrealm", new PolarisCallContext(null, null)))) {
      EntityCache cache2 = serviceLocator.getService(EntityCache.class);
      assertThat(cache1).isNotSameAs(cache2);
    }

    // but if we start a new call context with the original realm, we'll get the same EntityCache
    // instance
    try (CallContext ctx =
        CallContext.setCurrentContext(
            CallContext.of(() -> realm, new PolarisCallContext(null, null)))) {
      EntityCache cache2 = serviceLocator.getService(EntityCache.class);
      assertThat(cache1).isSameAs(cache2);
    }
  }

  @Test
  public void testCacheForRealm() {
    // create a catalog
    String catalogName = "mycachecatalog";
    ServiceLocator serviceLocator = accessor.getServiceLocator();
    listCatalogs();

    // check for the catalog - it should not exist
    // the service_admin role, however, should exist
    try (CallContext ctx =
        CallContext.setCurrentContext(
            CallContext.of(() -> realm, new PolarisCallContext(null, null)))) {
      EntityCache cache = serviceLocator.getService(EntityCache.class);
      assertThat(cache).isNotNull();
      EntityCacheEntry cachedCatalog =
          cache.getEntityByName(new EntityCacheByNameKey(PolarisEntityType.CATALOG, catalogName));
      assertThat(cachedCatalog).isNull();
      EntityCacheEntry serviceAdmin =
          cache.getEntityByName(
              new EntityCacheByNameKey(
                  PolarisEntityType.PRINCIPAL_ROLE,
                  PolarisEntityConstants.getNameOfPrincipalServiceAdminRole()));
      assertThat(serviceAdmin)
          .isNotNull()
          .extracting(EntityCacheEntry::getEntity)
          .returns(PolarisEntityType.PRINCIPAL_ROLE, PolarisBaseEntity::getType)
          .returns(
              PolarisEntityConstants.getNameOfPrincipalServiceAdminRole(),
              PolarisBaseEntity::getName);
    }
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(catalogName)
            .setStorageConfigInfo(
                new AwsStorageConfigInfo(
                    "arn:aws:iam::012345678901:role/jdoe", StorageConfigInfo.StorageTypeEnum.S3))
            .setProperties(new CatalogProperties("s3://bucket1/"))
            .build();
    createCatalog(catalog);
    createCatalogRole(catalogName, "my_cr", userToken);

    // now check again for the catalog - it should exist
    try (CallContext ctx =
        CallContext.setCurrentContext(
            CallContext.of(() -> realm, new PolarisCallContext(null, null)))) {
      EntityCache cache = serviceLocator.getService(EntityCache.class);
      assertThat(cache).isNotNull();
      EntityCacheEntry cachedCatalog =
          cache.getEntityByName(new EntityCacheByNameKey(PolarisEntityType.CATALOG, catalogName));
      assertThat(cachedCatalog)
          .isNotNull()
          .extracting(EntityCacheEntry::getEntity)
          .returns(catalogName, PolarisBaseEntity::getName);
    }

    // but if we check a different realm, the catalog should not exist in the cache
    // the service_admin role also does not exist, since it's never been used
    try (CallContext ctx =
        CallContext.setCurrentContext(
            CallContext.of(() -> "another-realm", new PolarisCallContext(null, null)))) {
      EntityCache cache = serviceLocator.getService(EntityCache.class);
      assertThat(cache).isNotNull();
      EntityCacheEntry cachedCatalog =
          cache.getEntityByName(new EntityCacheByNameKey(PolarisEntityType.CATALOG, catalogName));
      assertThat(cachedCatalog).isNull();
      EntityCacheEntry serviceAdmin =
          cache.getEntityByName(
              new EntityCacheByNameKey(
                  PolarisEntityType.PRINCIPAL_ROLE,
                  PolarisEntityConstants.getNameOfPrincipalServiceAdminRole()));
      assertThat(serviceAdmin).isNull();
    }
  }

  private static Invocation.Builder newRequest(String url, String token) {
    return EXT.client()
        .target(String.format(url, EXT.getLocalPort()))
        .request("application/json")
        .header("Authorization", "Bearer " + token)
        .header(REALM_PROPERTY_KEY, realm);
  }

  private static Invocation.Builder newRequest(String url) {
    return newRequest(url, userToken);
  }

  private static void listCatalogs() {
    try (Response response = newRequest("http://localhost:%d/api/management/v1/catalogs").get()) {
      assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    }
  }

  private static void createCatalog(Catalog catalog) {
    try (Response response =
        newRequest("http://localhost:%d/api/management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(catalog)))) {

      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  private static void createCatalogRole(
      String catalogName, String catalogRoleName, String catalogAdminToken) {
    try (Response response =
        newRequest(
                "http://localhost:%d/api/management/v1/catalogs/" + catalogName + "/catalog-roles",
                catalogAdminToken)
            .post(Entity.json(new CreateCatalogRoleRequest(new CatalogRole(catalogRoleName))))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }
}
