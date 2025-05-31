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

package org.apache.polaris.core.utils;

import java.util.function.Supplier;
import org.apache.polaris.core.context.RealmContext;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class CachedSupplierTest {
  private String realmName = "test";
  private int timesCalled = 0;
  private final RealmContext realmContext =
      new RealmContext() {
        @Override
        public String getRealmIdentifier() {
          if (++timesCalled == 1) {
            return realmName;
          }
          throw new IllegalStateException();
        }
      };

  private static class ContainerRealmIdentifier {
    private String realmIdentifier;

    public ContainerRealmIdentifier(RealmContext realmContext) {
      this.realmIdentifier = realmContext.getRealmIdentifier();
    }

    public String getRealmIdentifier() {
      return realmIdentifier;
    }
  }

  @Test
  public void testCachedSupplier() {
    Supplier<ContainerRealmIdentifier> realmIdentifierSupplier =
        () -> new ContainerRealmIdentifier(realmContext);
    Assertions.assertThat(realmName.equals(realmIdentifierSupplier.get().getRealmIdentifier()))
        .isTrue(); // This will work
    Assertions.assertThatThrownBy(() -> realmIdentifierSupplier.get().getRealmIdentifier())
        .isInstanceOf(IllegalStateException.class);

    timesCalled = 0;
    CachedSupplier<ContainerRealmIdentifier> cachedSupplier =
        new CachedSupplier<>(() -> new ContainerRealmIdentifier(realmContext));
    Assertions.assertThat(realmName.equals(cachedSupplier.get().getRealmIdentifier()))
        .isTrue(); // This will work
    Assertions.assertThat(realmName.equals(cachedSupplier.get().getRealmIdentifier()))
        .isTrue(); // This will work
  }
}
