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

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.config.DefaultConfigurationStore;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@QuarkusTest
@TestProfile(DefaultConfigurationStoreTest.Profile.class)
public class DefaultConfigurationStoreTest {
  private static final String falseByDefaultKey = "ALLOW_SPECIFYING_FILE_IO_IMPL";
  private static final String trueByDefaultKey1 = "ENABLE_GENERIC_TABLES";
  private static final String trueByDefaultKey2 = "ENABLE_POLICY_STORE";
  private static final String realmOne = "realm1";
  private static final String realmTwo = "realm2";

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.realm-context.realms",
          "realm1,realm2",
          String.format("polaris.features.\"%s\"", falseByDefaultKey),
          "true",
          String.format("polaris.features.\"%s\"", trueByDefaultKey1),
          "false",
          String.format("polaris.features.realm-overrides.\"%s\".\"%s\"", realmOne, falseByDefaultKey),
          "false");
    }
  }

  private PolarisCallContext polarisContext;

  @Inject MetaStoreManagerFactory managerFactory;
  @Inject PolarisConfigurationStore configurationStore;
  @Inject PolarisDiagnostics diagServices;

  @BeforeEach
  public void before(TestInfo testInfo) {
    String realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(java.lang.reflect.Method::getName).orElse("test"),
                System.nanoTime());
    RealmContext realmContext = () -> realmName;
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    polarisContext =
        new PolarisCallContext(
            managerFactory.getOrCreateSessionSupplier(realmContext).get(),
            diagServices,
            configurationStore,
            Clock.systemDefaultZone());
  }

  @Test
  public void testGetConfiguration() {
    DefaultConfigurationStore defaultConfigurationStore =
        new DefaultConfigurationStore(Map.of("key1", 1, "key2", "value"));
    InMemoryPolarisMetaStoreManagerFactory metastoreFactory =
        new InMemoryPolarisMetaStoreManagerFactory();
    PolarisCallContext callCtx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> "realm1").get(),
            new PolarisDefaultDiagServiceImpl());
    Object value = defaultConfigurationStore.getConfiguration(callCtx, "missingKeyWithoutDefault");
    assertThat(value).isNull();
    Object defaultValue =
        defaultConfigurationStore.getConfiguration(
            callCtx, "missingKeyWithDefault", "defaultValue");
    assertThat(defaultValue).isEqualTo("defaultValue");
    Integer keyOne = defaultConfigurationStore.getConfiguration(callCtx, "key1");
    assertThat(keyOne).isEqualTo(1);
    String keyTwo = defaultConfigurationStore.getConfiguration(callCtx, "key2");
    assertThat(keyTwo).isEqualTo("value");

    boolean keyOneValue = configurationStore.getConfiguration(polarisContext, falseByDefaultKey);
    assertThat(keyOneValue).isTrue();
    boolean keyTwoValue = configurationStore.getConfiguration(polarisContext, trueByDefaultKey1);
    assertThat(keyTwoValue).isFalse();
    boolean keyThreeValue = configurationStore.getConfiguration(polarisContext, trueByDefaultKey2);
    assertThat(keyThreeValue).isTrue();
  }

  @Test
  public void testGetDefaultConfigurations() {
    Object value = configurationStore.getConfiguration(polarisContext, "missingKeyWithoutDefault");
    assertThat(value).isNull();

    Object defaultValue =
        configurationStore.getConfiguration(
            polarisContext, "missingKeyWithDefault", "defaultValue");
    assertThat(defaultValue).isEqualTo("defaultValue");

    boolean keyOneValue = configurationStore.getConfiguration(polarisContext, falseByDefaultKey);
    assertThat(keyOneValue).isFalse();
    boolean keyTwoValue = configurationStore.getConfiguration(polarisContext, trueByDefaultKey1);
    assertThat(keyTwoValue).isTrue();
    boolean keyThreeValue = configurationStore.getConfiguration(polarisContext, trueByDefaultKey2);
    assertThat(keyThreeValue).isTrue();
  }

  @Test
  public void testGetRealmConfiguration() {
    RealmContext realmContext = () -> realmOne;
    QuarkusMock.installMockForType(realmContext, RealmContext.class);

    boolean keyOneValue = configurationStore.getConfiguration(polarisContext, falseByDefaultKey);
    assertThat(keyOneValue).isTrue();

    realmContext = () -> realmTwo;
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    keyOneValue = configurationStore.getConfiguration(polarisContext, falseByDefaultKey);
    assertThat(keyOneValue).isFalse();
  }
}
