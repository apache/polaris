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
package org.apache.polaris.core.persistence.cache;

import static org.apache.polaris.core.policy.content.PolicyContentUtil.MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class EntityWeigherTest {

  private PolarisDiagnostics diagnostics;

  public EntityWeigherTest() {
    diagnostics = new PolarisDefaultDiagServiceImpl();
  }

  private ResolvedPolarisEntity getEntity(
      String name,
      String metadataLocation,
      String properties,
      Optional<String> internalProperties) {
    Map<String, String> propertiesMap = getPropertiesMap(properties);
    var entity = new IcebergTableLikeEntity.Builder(TableIdentifier.of(name), metadataLocation);
    entity.setProperties(propertiesMap);
    internalProperties.ifPresent(
        p -> {
          Map<String, String> internalPropertiesMap = getPropertiesMap(p);
          entity.setInternalProperties(internalPropertiesMap);
        });
    return new ResolvedPolarisEntity(diagnostics, entity.build(), List.of(), 1);
  }

  @Test
  public void testBasicWeight() {
    int weight = EntityWeigher.getInstance().weigh(1L, getEntity("t", "", "", Optional.empty()));
    Assertions.assertThat(weight).isGreaterThan(0);
  }

  @Test
  public void testNonZeroWeight() {
    int weight = EntityWeigher.getInstance().weigh(1L, getEntity("t", "", "", Optional.of("")));
    Assertions.assertThat(weight).isGreaterThan(0);
  }

  @Test
  public void testWeightIncreasesWithNameLength() {
    int smallWeight =
        EntityWeigher.getInstance().weigh(1L, getEntity("t", "", "", Optional.empty()));
    int largeWeight =
        EntityWeigher.getInstance()
            .weigh(1L, getEntity("{\"v\":\"looong properties\"}", "", "", Optional.empty()));
    Assertions.assertThat(smallWeight).isLessThan(largeWeight);
  }

  @Test
  public void testWeightIncreasesWithMetadataLocationLength() {
    int smallWeight =
        EntityWeigher.getInstance().weigh(1L, getEntity("t", "", "", Optional.empty()));
    int largeWeight =
        EntityWeigher.getInstance()
            .weigh(1L, getEntity("t", "looong location", "", Optional.empty()));
    Assertions.assertThat(smallWeight).isLessThan(largeWeight);
  }

  @Test
  public void testWeightIncreasesWithPropertiesLength() {
    int smallWeight =
        EntityWeigher.getInstance().weigh(1L, getEntity("t", "", "", Optional.empty()));
    int largeWeight =
        EntityWeigher.getInstance()
            .weigh(1L, getEntity("t", "", "{\"v\":\"looong properties\"}", Optional.empty()));
    Assertions.assertThat(smallWeight).isLessThan(largeWeight);
  }

  @Test
  public void testWeightIncreasesWithInternalPropertiesLength() {
    int smallWeight =
        EntityWeigher.getInstance().weigh(1L, getEntity("t", "", "", Optional.of("")));
    int largeWeight =
        EntityWeigher.getInstance()
            .weigh(1L, getEntity("t", "", "", Optional.of("{\"v\":\"looong iproperties\"}")));
    Assertions.assertThat(smallWeight).isLessThan(largeWeight);
  }

  @Test
  public void testExactWeightCalculation() {
    int preciseWeight =
        EntityWeigher.getInstance()
            .weigh(
                1L,
                getEntity(
                    "name",
                    "location",
                    "{\"a\": \"b\"}",
                    Optional.of("{\"c\": \"d\", \"e\": \"f\"}")));
    Assertions.assertThat(preciseWeight).isEqualTo(1090);
  }

  private static Map<String, String> getPropertiesMap(String properties) {
    if (properties == null || properties.isEmpty()) return new HashMap<>();
    Map<String, String> propertiesMap;
    try {
      propertiesMap = MAPPER.readValue(properties, new TypeReference<>() {});
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return propertiesMap;
  }
}
