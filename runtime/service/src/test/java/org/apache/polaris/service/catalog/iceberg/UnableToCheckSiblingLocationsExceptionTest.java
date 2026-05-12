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

package org.apache.polaris.service.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog.UnableToCheckSiblingLocationsException;
import org.junit.jupiter.api.Test;

class UnableToCheckSiblingLocationsExceptionTest {

  @Test
  void testEntityNotResolvedMessage() {
    ResolverStatus s = new ResolverStatus(PolarisEntityType.TABLE_LIKE, "test-name");
    UnableToCheckSiblingLocationsException e = new UnableToCheckSiblingLocationsException(s);
    assertThat(e).hasMessageContaining("Unable to resolve sibling entities to validate location");
    assertThat(e).hasMessageContaining("test-name");
    assertThat(e)
        .hasMessageContaining(ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED.name());
  }

  @Test
  void testPathNotResolvedMessage() {
    ResolverPath path =
        new ResolverPath(List.of("ns1", "ns2", "table3"), PolarisEntityType.TABLE_LIKE, true);
    ResolverStatus s = new ResolverStatus(path, 123);
    UnableToCheckSiblingLocationsException e = new UnableToCheckSiblingLocationsException(s);
    assertThat(e).hasMessageContaining("Unable to resolve sibling entities to validate location");
    assertThat(e).hasMessageContaining("ns1.ns2.table3");
    assertThat(e).hasMessageContaining("failed index: 123");
    assertThat(e)
        .hasMessageContaining(ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED.name());
  }
}
