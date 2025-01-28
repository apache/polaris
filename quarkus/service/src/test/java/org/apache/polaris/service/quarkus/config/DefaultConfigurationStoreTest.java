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

import java.util.Map;
import org.apache.polaris.core.context.Realm;
import org.apache.polaris.service.config.DefaultConfigurationStore;
import org.junit.jupiter.api.Test;

public class DefaultConfigurationStoreTest {

  @Test
  public void testGetConfiguration() {
    DefaultConfigurationStore defaultConfigurationStore =
        new DefaultConfigurationStore(Map.of("key1", 1, "key2", "value"));
    Realm realm = Realm.newRealm("test");
    Object value = defaultConfigurationStore.getConfiguration(realm, "missingKeyWithoutDefault");
    assertThat(value).isNull();
    Object defaultValue =
        defaultConfigurationStore.getConfiguration(realm, "missingKeyWithDefault", "defaultValue");
    assertThat(defaultValue).isEqualTo("defaultValue");
    Integer keyOne = defaultConfigurationStore.getConfiguration(realm, "key1");
    assertThat(keyOne).isEqualTo(1);
    String keyTwo = defaultConfigurationStore.getConfiguration(realm, "key2");
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

    // check realm1 values
    Realm realm = Realm.newRealm("realm1");
    Object value = defaultConfigurationStore.getConfiguration(realm, "missingKeyWithoutDefault");
    assertThat(value).isNull();
    Object defaultValue =
        defaultConfigurationStore.getConfiguration(realm, "missingKeyWithDefault", "defaultValue");
    assertThat(defaultValue).isEqualTo("defaultValue");
    Integer keyOneRealm1 = defaultConfigurationStore.getConfiguration(realm, "key1");
    assertThat(keyOneRealm1).isEqualTo(realm1KeyOneValue);
    String keyTwoRealm1 = defaultConfigurationStore.getConfiguration(realm, "key2");
    assertThat(keyTwoRealm1).isEqualTo(defaultKeyTwoValue);

    // check realm2 values
    realm = Realm.newRealm("realm2");
    Integer keyOneRealm2 = defaultConfigurationStore.getConfiguration(realm, "key1");
    assertThat(keyOneRealm2).isEqualTo(realm2KeyOneValue);
    String keyTwoRealm2 = defaultConfigurationStore.getConfiguration(realm, "key2");
    assertThat(keyTwoRealm2).isEqualTo(realm2KeyTwoValue);

    // realm3 has no realm-overrides, so just returns default values
    realm = Realm.newRealm("realm3");
    Integer keyOneRealm3 = defaultConfigurationStore.getConfiguration(realm, "key1");
    assertThat(keyOneRealm3).isEqualTo(defaultKeyOneValue);
    String keyTwoRealm3 = defaultConfigurationStore.getConfiguration(realm, "key2");
    assertThat(keyTwoRealm3).isEqualTo(defaultKeyTwoValue);
  }
}
