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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.junit.jupiter.api.Test;

class DefaultConfigurationStoreTest {

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
    Integer keyOneRealm1 = defaultConfigurationStore.getConfiguration(realm1Ctx, "key1");
    assertThat(keyOneRealm1).isEqualTo(realm1KeyOneValue);
    String keyTwoRealm1 = defaultConfigurationStore.getConfiguration(realm1Ctx, "key2");
    assertThat(keyTwoRealm1).isEqualTo(defaultKeyTwoValue);

    // check realm2 values
    PolarisCallContext realm2Ctx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> "realm2").get(),
            new PolarisDefaultDiagServiceImpl());
    Integer keyOneRealm2 = defaultConfigurationStore.getConfiguration(realm2Ctx, "key1");
    assertThat(keyOneRealm2).isEqualTo(realm2KeyOneValue);
    String keyTwoRealm2 = defaultConfigurationStore.getConfiguration(realm2Ctx, "key2");
    assertThat(keyTwoRealm2).isEqualTo(realm2KeyTwoValue);

    // realm3 has no realm-overrides, so just returns default values
    PolarisCallContext realm3Ctx =
        new PolarisCallContext(
            metastoreFactory.getOrCreateSessionSupplier(() -> "realm3").get(),
            new PolarisDefaultDiagServiceImpl());
    Integer keyOneRealm3 = defaultConfigurationStore.getConfiguration(realm3Ctx, "key1");
    assertThat(keyOneRealm3).isEqualTo(defaultKeyOneValue);
    String keyTwoRealm3 = defaultConfigurationStore.getConfiguration(realm3Ctx, "key2");
    assertThat(keyTwoRealm3).isEqualTo(defaultKeyTwoValue);
  }
}
