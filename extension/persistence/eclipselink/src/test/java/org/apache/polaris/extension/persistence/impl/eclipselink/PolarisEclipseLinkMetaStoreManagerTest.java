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
package org.apache.polaris.extension.persistence.impl.eclipselink;

import static jakarta.persistence.Persistence.createEntityManagerFactory;
import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.BasePolarisMetaStoreManagerTest;
import org.apache.polaris.core.persistence.PolarisMetaStoreManagerImpl;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.jpa.models.ModelPrincipalSecrets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mockito;

/**
 * Integration test for EclipseLink based metastore implementation
 *
 * @author aixu
 */
public class PolarisEclipseLinkMetaStoreManagerTest extends BasePolarisMetaStoreManagerTest {

  @Override
  protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisEclipseLinkStore store = new PolarisEclipseLinkStore(diagServices);
    PolarisEclipseLinkMetaStoreSessionImpl session =
        new PolarisEclipseLinkMetaStoreSessionImpl(
            store, Mockito.mock(), () -> "realm", null, "polaris", RANDOM_SECRETS);
    return new PolarisTestMetaStoreManager(
        new PolarisMetaStoreManagerImpl(),
        new PolarisCallContext(
            session,
            diagServices,
            new PolarisConfigurationStore() {},
            timeSource.withZone(ZoneId.systemDefault())));
  }

  @ParameterizedTest()
  @ArgumentsSource(CreateStoreSessionArgs.class)
  void testCreateStoreSession(String confFile, boolean success) {
    // Clear cache to prevent reuse EntityManagerFactory
    PolarisEclipseLinkMetaStoreSessionImpl.clearEntityManagerFactories();

    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    PolarisEclipseLinkStore store = new PolarisEclipseLinkStore(diagServices);
    try {
      var session =
          new PolarisEclipseLinkMetaStoreSessionImpl(
              store, Mockito.mock(), () -> "realm", confFile, "polaris", RANDOM_SECRETS);
      assertNotNull(session);
      assertTrue(success);
    } catch (Exception e) {
      assertFalse(success);
    }
  }

  @Test
  void testRotateLegacyPrincipalSecret() {

    PolarisEclipseLinkMetaStoreSessionImpl.clearEntityManagerFactories();

    var newSecrets = new PolarisPrincipalSecrets(42L);
    assertThat(newSecrets)
        .extracting(
            PolarisPrincipalSecrets::getMainSecret,
            PolarisPrincipalSecrets::getSecondarySecret,
            PolarisPrincipalSecrets::getMainSecretHash,
            PolarisPrincipalSecrets::getSecondarySecretHash,
            PolarisPrincipalSecrets::getSecretSalt)
        .doesNotContainNull()
        .allMatch(x -> !x.toString().isEmpty());

    ModelPrincipalSecrets model = ModelPrincipalSecrets.fromPrincipalSecrets(newSecrets);
    var key = model.getPrincipalClientId();

    var fromModel = ModelPrincipalSecrets.toPrincipalSecrets(model);

    assertThat(fromModel)
        .extracting(
            PolarisPrincipalSecrets::getMainSecret, PolarisPrincipalSecrets::getSecondarySecret)
        .containsOnlyNulls();

    assertThat(model)
        .extracting(
            ModelPrincipalSecrets::getPrincipalId,
            ModelPrincipalSecrets::getPrincipalClientId,
            ModelPrincipalSecrets::getSecretSalt,
            ModelPrincipalSecrets::getMainSecretHash,
            ModelPrincipalSecrets::getSecondarySecretHash)
        .containsExactly(
            newSecrets.getPrincipalId(),
            newSecrets.getPrincipalClientId(),
            newSecrets.getSecretSalt(),
            newSecrets.getMainSecretHash(),
            newSecrets.getSecondarySecretHash());

    try (var emf = createEntityManagerFactory("polaris")) {
      var entityManager = emf.createEntityManager();

      // Persist the original model:
      entityManager.getTransaction().begin();
      entityManager.persist(model);
      entityManager.getTransaction().commit();

      // Retrieve the model
      entityManager.clear();
      ModelPrincipalSecrets retrievedModel = entityManager.find(ModelPrincipalSecrets.class, key);

      assertThat(retrievedModel)
          .extracting(
              ModelPrincipalSecrets::getPrincipalId,
              ModelPrincipalSecrets::getPrincipalClientId,
              ModelPrincipalSecrets::getSecretSalt,
              ModelPrincipalSecrets::getMainSecretHash,
              ModelPrincipalSecrets::getSecondarySecretHash)
          .containsExactly(
              model.getPrincipalId(),
              model.getPrincipalClientId(),
              model.getSecretSalt(),
              model.getMainSecretHash(),
              model.getSecondarySecretHash());

      // Now read using PolarisEclipseLinkStore
      var store = new PolarisEclipseLinkStore(new PolarisDefaultDiagServiceImpl());
      store.initialize(entityManager);
      PolarisPrincipalSecrets principalSecrets =
          ModelPrincipalSecrets.toPrincipalSecrets(
              store.lookupPrincipalSecrets(entityManager, key));

      assertThat(principalSecrets)
          .extracting(
              PolarisPrincipalSecrets::getPrincipalId,
              PolarisPrincipalSecrets::getPrincipalClientId,
              PolarisPrincipalSecrets::getSecretSalt,
              PolarisPrincipalSecrets::getMainSecret,
              PolarisPrincipalSecrets::getMainSecretHash,
              PolarisPrincipalSecrets::getSecondarySecret,
              PolarisPrincipalSecrets::getSecondarySecretHash)
          .containsExactly(
              fromModel.getPrincipalId(),
              fromModel.getPrincipalClientId(),
              fromModel.getSecretSalt(),
              null,
              fromModel.getMainSecretHash(),
              null,
              fromModel.getSecondarySecretHash());

      // Rotate:
      principalSecrets.rotateSecrets(principalSecrets.getMainSecretHash());
      assertThat(principalSecrets.getMainSecret()).isNotEqualTo(newSecrets.getMainSecret());
      assertThat(principalSecrets.getMainSecretHash()).isNotEqualTo(newSecrets.getMainSecretHash());
      assertThat(principalSecrets)
          .extracting(
              PolarisPrincipalSecrets::getSecondarySecret,
              PolarisPrincipalSecrets::getSecondarySecretHash)
          .containsExactly(null, newSecrets.getMainSecretHash());

      // Persist the rotated credential:
      store.deletePrincipalSecrets(entityManager, key);
      store.writePrincipalSecrets(entityManager, principalSecrets);

      // Reload the model:
      var reloadedModel = store.lookupPrincipalSecrets(entityManager, key);

      // Confirm the old secret still works via hash:
      var reloadedSecrets = ModelPrincipalSecrets.toPrincipalSecrets(reloadedModel);
      Assertions.assertTrue(reloadedSecrets.matchesSecret(newSecrets.getMainSecret()));
      Assertions.assertFalse(reloadedSecrets.matchesSecret(newSecrets.getSecondarySecret()));
    }
  }

  private static class CreateStoreSessionArgs implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws URISyntaxException {
      Path persistenceXml =
          Paths.get(
              Objects.requireNonNull(getClass().getResource("/META-INF/persistence.xml")).toURI());
      Path confJar =
          Paths.get(
              Objects.requireNonNull(getClass().getResource("/eclipselink/test-conf.jar")).toURI());
      return Stream.of(
          // conf file not provided
          Arguments.of(null, true),
          // classpath resource
          Arguments.of("META-INF/persistence.xml", true),
          Arguments.of("META-INF/dummy.xml", false),
          // classpath resource, embedded
          Arguments.of("eclipselink/test-conf.jar!/persistence.xml", true),
          Arguments.of("eclipselink/test-conf.jar!/dummy.xml", false),
          Arguments.of("dummy/test-conf.jar!/persistence.xml", false),
          // filesystem path
          Arguments.of(persistenceXml.toString(), true),
          Arguments.of("/dummy_path/conf/persistence.xml", false),
          // filesystem path, embedded
          Arguments.of(confJar + "!/persistence.xml", true),
          Arguments.of(confJar + "!/dummy.xml", false),
          Arguments.of("/dummy_path/test-conf.jar!/persistence.xml", false));
    }
  }
}
