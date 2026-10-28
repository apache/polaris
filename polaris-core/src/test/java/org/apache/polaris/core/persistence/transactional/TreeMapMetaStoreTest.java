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
package org.apache.polaris.core.persistence.transactional;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.junit.jupiter.api.Test;

class TreeMapMetaStoreTest {

  private final PolarisDiagnostics diagnostics = new PolarisDefaultDiagServiceImpl();

  @Test
  void readRangeReturnsCopiesForEmptyPrefix() {
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    PolarisGrantRecord grantRecord = new PolarisGrantRecord(1L, 2L, 3L, 4L, 5);

    store.runActionInTransaction(
        diagnostics, () -> store.getSliceGrantRecords().write(grantRecord));

    List<PolarisGrantRecord> range =
        store.runInReadTransaction(diagnostics, () -> store.getSliceGrantRecords().readRange(""));
    range.get(0).setPrivilegeCode(99);

    PolarisGrantRecord stored =
        store.runInReadTransaction(
            diagnostics, () -> store.getSliceGrantRecords().readRange("").get(0));

    assertThat(stored.getPrivilegeCode()).isEqualTo(5);
  }

  @Test
  void readRangeReturnsCopiesForNonEmptyPrefix() {
    TreeMapMetaStore store = new TreeMapMetaStore(diagnostics);
    PolarisGrantRecord grantRecord = new PolarisGrantRecord(1L, 2L, 3L, 4L, 5);
    String prefix = store.buildPrefixKeyComposite(1L, 2L);

    store.runActionInTransaction(
        diagnostics, () -> store.getSliceGrantRecords().write(grantRecord));

    List<PolarisGrantRecord> range =
        store.runInReadTransaction(
            diagnostics, () -> store.getSliceGrantRecords().readRange(prefix));
    range.get(0).setPrivilegeCode(99);

    PolarisGrantRecord stored =
        store.runInReadTransaction(
            diagnostics, () -> store.getSliceGrantRecords().readRange(prefix).get(0));

    assertThat(stored.getPrivilegeCode()).isEqualTo(5);
  }
}
