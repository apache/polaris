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
package org.apache.polaris.service.catalog.io;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import jakarta.annotation.Nonnull;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmId;
import org.apache.polaris.core.entity.*;
import org.apache.polaris.core.persistence.*;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.service.config.DefaultConfigurationStore;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class FileIOFactoryTest {

  public static final String TEST_ACCESS_KEY = "test_access_key";
  public static final String SECRET_ACCESS_KEY = "secret_access_key";
  public static final String SESSION_TOKEN = "session_token";

  private RealmEntityManagerFactory realmEntityManagerFactory;
  private PolarisStorageIntegrationProvider storageIntegrationProvider;
  private MetaStoreManagerFactory metaStoreManagerFactory;
  private PolarisDiagnostics polarisDiagnostics;
  private PolarisEntityManager entityManager;
  private PolarisMetaStoreManager metaStoreManager;
  private PolarisMetaStoreSession metaStoreSession;
  private PolarisConfigurationStore configurationStore;

  private RealmId realmId;

  @BeforeEach
  public void before(TestInfo testInfo) {
    String realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    realmId = RealmId.newRealmId(realmName);
    configurationStore = new DefaultConfigurationStore(Map.of());
    polarisDiagnostics = Mockito.mock(PolarisDiagnostics.class);

    StsClient stsClient = Mockito.mock(StsClient.class);
    storageIntegrationProvider =
        new PolarisStorageIntegrationProviderImpl(
            () -> stsClient,
            () -> GoogleCredentials.create(new AccessToken("abc", new Date())),
            configurationStore);
    metaStoreManagerFactory =
        new InMemoryPolarisMetaStoreManagerFactory(
            storageIntegrationProvider,
            configurationStore,
            polarisDiagnostics,
            Clock.systemDefaultZone());
    realmEntityManagerFactory =
        new RealmEntityManagerFactory(metaStoreManagerFactory, polarisDiagnostics) {};
    entityManager = realmEntityManagerFactory.getOrCreateEntityManager(realmId);
    metaStoreManager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmId);
    metaStoreSession = metaStoreManagerFactory.getOrCreateSessionSupplier(realmId).get();

    // Mock get subscoped creds
    when(stsClient.assumeRole(isA(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId(TEST_ACCESS_KEY)
                        .secretAccessKey(SECRET_ACCESS_KEY)
                        .sessionToken(SESSION_TOKEN)
                        .build())
                .build());
  }

  @AfterEach
  public void after() {
    metaStoreManager.purge(metaStoreSession);
  }

  @Test
  public void testLoadFileIOForTableLike() {
    String storageLocation = "s3://my-bucket/path/to/data";
    AwsStorageConfigurationInfo storageConfig =
        new AwsStorageConfigurationInfo(
            PolarisStorageConfigurationInfo.StorageType.S3,
            List.of(storageLocation, "s3://externally-owned-bucket"),
            "arn:aws:iam::012345678901:role/jdoe",
            null,
            null);
    ResolvedPolarisEntity catalogEntity =
        createResolvedEntity(
            10,
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            10,
            0,
            "my-catalog",
            Map.of(
                PolarisEntityConstants.getStorageConfigInfoPropertyName(),
                storageConfig.serialize()));
    ResolvedPolarisEntity tableEntity =
        createResolvedEntity(
            10,
            PolarisEntityType.TABLE_LIKE,
            PolarisEntitySubType.TABLE,
            11,
            10,
            "my-table",
            Map.of());
    metaStoreManager.createCatalog(metaStoreSession, catalogEntity.getEntity(), List.of());
    PolarisResolvedPathWrapper resolvedPathWrapper =
        new PolarisResolvedPathWrapper(List.of(catalogEntity, tableEntity));

    String testProperty = "test.property";
    FileIOFactory fileIOFactory =
        new DefaultFileIOFactory(
            realmEntityManagerFactory, metaStoreManagerFactory, configurationStore) {
          @Override
          FileIO loadFileIOInternal(
              @Nonnull String ioImplClassName, @Nonnull Map<String, String> properties) {
            // properties should contain credentials
            org.assertj.core.api.Assertions.assertThat(properties)
                .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, TEST_ACCESS_KEY)
                .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, SECRET_ACCESS_KEY)
                .containsEntry(S3FileIOProperties.SESSION_TOKEN, SESSION_TOKEN)
                .containsEntry(testProperty, "true");
            return super.loadFileIOInternal(ioImplClassName, properties);
          }
        };
    fileIOFactory
        .loadFileIO(
            realmId,
            InMemoryFileIO.class.getName(),
            Map.of(testProperty, "true"),
            TableIdentifier.of("my-ns", "my-table"),
            Set.of(storageLocation),
            Set.of(PolarisStorageActions.READ),
            resolvedPathWrapper)
        .close();
  }

  private ResolvedPolarisEntity createResolvedEntity(
      long catalogId,
      PolarisEntityType type,
      PolarisEntitySubType subType,
      long id,
      long parentId,
      String name,
      Map<String, String> internalProperties) {
    PolarisEntity polarisEntity =
        new PolarisEntity(
            catalogId,
            type,
            subType,
            id,
            parentId,
            name,
            0,
            0,
            0,
            0,
            Map.of(),
            internalProperties,
            0,
            0);
    return new ResolvedPolarisEntity(polarisEntity, List.of(), List.of());
  }
}
