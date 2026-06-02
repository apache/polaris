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
package org.apache.polaris.persistence.nosql.inmemory;

import static org.apache.polaris.persistence.nosql.api.backend.PersistId.persistId;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.Test;

public class TestInMemoryBackend {
  @Test
  public void serializedValuesAreCopiedOnWriteAndRead() {
    var backend = new InMemoryBackend();
    var id = persistId(1234L, 0);
    var serialized = new byte[] {1, 2, 3};

    assertThat(backend.conditionalInsert("realm", "type", id, 42L, "v1", serialized)).isTrue();

    serialized[0] = 9;

    var fetched = backend.fetch("realm", Set.of(id)).get(id);
    assertThat(fetched.serialized()).containsExactly(1, 2, 3);

    fetched.serialized()[1] = 8;

    var fetchedAgain = backend.fetch("realm", Set.of(id)).get(id);
    assertThat(fetchedAgain.serialized()).containsExactly(1, 2, 3);
  }
}
