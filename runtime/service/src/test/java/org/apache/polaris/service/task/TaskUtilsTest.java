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
package org.apache.polaris.service.task;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation") // exercises the legacy STORAGE_LOCATION key
public class TaskUtilsTest {

  @Test
  public void testStorageLocationsRoundTrip() {
    Set<String> locations = Set.of("s3://bucket/table", "s3://bucket/custom-data");
    Map<String, String> properties =
        Map.of(PolarisTaskConstants.STORAGE_LOCATIONS, TaskUtils.encodeStorageLocations(locations));

    assertThat(TaskUtils.storageLocations(properties)).isEqualTo(locations);
  }

  @Test
  public void testStorageLocationsFallsBackToLegacySingleLocation() {
    Map<String, String> properties =
        Map.of(PolarisTaskConstants.STORAGE_LOCATION, "s3://bucket/table");

    assertThat(TaskUtils.storageLocations(properties)).containsExactly("s3://bucket/table");
  }

  @Test
  public void testStorageLocationsPrefersNewKeyOverLegacy() {
    Map<String, String> properties =
        Map.of(
            PolarisTaskConstants.STORAGE_LOCATION,
            "s3://bucket/legacy",
            PolarisTaskConstants.STORAGE_LOCATIONS,
            TaskUtils.encodeStorageLocations(Set.of("s3://bucket/new")));

    assertThat(TaskUtils.storageLocations(properties)).containsExactly("s3://bucket/new");
  }

  @Test
  public void testStorageLocationsEmptyWhenNothingRecorded() {
    assertThat(TaskUtils.storageLocations(Map.of())).isEmpty();
  }
}
