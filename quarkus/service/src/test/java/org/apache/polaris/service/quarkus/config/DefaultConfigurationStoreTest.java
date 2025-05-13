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
import org.apache.polaris.core.context.CallContext;
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
  private static final String trueByDefaultKey = "ENABLE_GENERIC_TABLES";
  private static final String realmOne = "realm1";

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.realm-context.realms",
          "realm1,realm2",
          String.format("polaris.features.\"%s\"", falseByDefaultKey),
          "true",
          String.format("polaris.features.\"%s\"", trueByDefaultKey),
          "false",
          String.format(
              "polaris.features.realm-overrides.\"%s\".\"%s\"", realmOne, falseByDefaultKey),
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
  }

  @Test
  public void testGetRealmConfiguration() {
    int defaultKeyOneValue = 1;
    String defaultKeyTwoValue = "value";

    int realm1KeyOneValue = 2;
    int realm2KeyOneValue = 3;
    String realm2KeyTwoValue = "value3";
    DefaultConfigurationStore defaultConfigurationStore =
        new DefaultConfigurationStore(
            Map.of("key1", defaultKeyOneValue, "key2", defaultKeyTwoValue),
            Map.of(
                "realm1",
                Map.of("key1", realm1KeyOneValue),
                "realm2",
                Map.of("key1", realm2KeyOneValue, "key2", realm2KeyTwoValue)));
    InMemoryPolarisMetaStoreManagerFactory metastoreFactory =
        new InMemoryPolarisMetaStoreManagerFactory();

    // check realm1 values
    PolarisCallContext realm1Ctx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> "realm1").get(),
            new PolarisDefaultDiagServiceImpl());
    Object value =
        defaultConfigurationStore.getConfiguration(realm1Ctx, "missingKeyWithoutDefault");
    assertThat(value).isNull();
    Object defaultValue =
        defaultConfigurationStore.getConfiguration(
            realm1Ctx, "missingKeyWithDefault", "defaultValue");
    assertThat(defaultValue).isEqualTo("defaultValue");
    CallContext.setCurrentContext(CallContext.of(() -> "realm1", realm1Ctx));
    Integer keyOneRealm1 = defaultConfigurationStore.getConfiguration(realm1Ctx, "key1");
    assertThat(keyOneRealm1).isEqualTo(realm1KeyOneValue);
    String keyTwoRealm1 = defaultConfigurationStore.getConfiguration(realm1Ctx, "key2");
    assertThat(keyTwoRealm1).isEqualTo(defaultKeyTwoValue);

    // check realm2 values
    PolarisCallContext realm2Ctx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> "realm2").get(),
            new PolarisDefaultDiagServiceImpl());
    CallContext.setCurrentContext(CallContext.of(() -> "realm2", realm2Ctx));
    Integer keyOneRealm2 = defaultConfigurationStore.getConfiguration(realm2Ctx, "key1");
    assertThat(keyOneRealm2).isEqualTo(realm2KeyOneValue);
    String keyTwoRealm2 = defaultConfigurationStore.getConfiguration(realm2Ctx, "key2");
    assertThat(keyTwoRealm2).isEqualTo(realm2KeyTwoValue);

    // realm3 has no realm-overrides, so just returns default values
    PolarisCallContext realm3Ctx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> "realm3").get(),
            new PolarisDefaultDiagServiceImpl());
    CallContext.setCurrentContext(CallContext.of(() -> "realm3", realm3Ctx));
    Integer keyOneRealm3 = defaultConfigurationStore.getConfiguration(realm3Ctx, "key1");
    assertThat(keyOneRealm3).isEqualTo(defaultKeyOneValue);
    String keyTwoRealm3 = defaultConfigurationStore.getConfiguration(realm3Ctx, "key2");
    assertThat(keyTwoRealm3).isEqualTo(defaultKeyTwoValue);
  }

  // TODO simplify once DefaultConfigrationStore doesn't rely on CallContext.getCurrentContext
  private void setCurrentRealm(String realmName) {
    RealmContext realmContext = () -> realmName;
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    CallContext.setCurrentContext(
        new CallContext() {
          @Override
          public RealmContext getRealmContext() {
            return realmContext;
          }

          @Override
          public PolarisCallContext getPolarisCallContext() {
            return CallContext.getCurrentContext().getPolarisCallContext();
          }

          @Override
          public Map<String, Object> contextVariables() {
            return CallContext.getCurrentContext().contextVariables();
          }
        });
  }

  @Test
  public void testInjectedConfigurationStore() {
    // Feature override makes this `false`
    boolean featureOverrideValue =
        configurationStore.getConfiguration(polarisContext, trueByDefaultKey);
    assertThat(featureOverrideValue).isFalse();

    // Feature override value makes this `true`
    setCurrentRealm("not-" + realmOne);
    boolean realmOverrideValue =
        configurationStore.getConfiguration(polarisContext, falseByDefaultKey);
    assertThat(realmOverrideValue).isTrue();

    // Now, realm override value makes this `false`
    setCurrentRealm(realmOne);
    realmOverrideValue = configurationStore.getConfiguration(polarisContext, falseByDefaultKey);
    assertThat(realmOverrideValue).isFalse();

    assertThat(configurationStore).isInstanceOf(DefaultConfigurationStore.class);
  }
}
