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
package org.apache.polaris.persistence.nosql.impl.indexes;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Var;
import java.util.Objects;
import org.apache.polaris.persistence.nosql.api.index.Index;

abstract class AbstractIndexElement<V> implements InternalIndexElement<V> {

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof InternalIndexElement<?> other) {
      return key().equals(other.key()) && Objects.equals(valueNullable(), other.valueNullable());
    }
    if (o instanceof Index.Element<?> other) {
      // This case is just for tests, not for actual production usage.
      return key().equals(other.key()) && Objects.equals(valueNullable(), other.value());
    }
    return false;
  }

  @Override
  public int hashCode() {
    @Var var h = 5381;
    h += (h << 5) + key().hashCode();
    var v = valueNullable();
    if (v != null) {
      h += (h << 5) + v.hashCode();
    }
    return h;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper("StoreIndexElement")
        .omitNullValues()
        .add("key", key())
        .add("content", valueNullable())
        .toString();
  }
}
