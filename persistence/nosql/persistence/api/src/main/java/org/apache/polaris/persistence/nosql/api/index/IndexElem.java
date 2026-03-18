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

package org.apache.polaris.persistence.nosql.api.index;

import jakarta.annotation.Nonnull;

/**
 * Package-private implementation of {@link Index.Element} for {@link Index.Element#of(IndexKey,
 * Object)}.
 */
final class IndexElem<V> implements Index.Element<V> {

  private final IndexKey key;
  private final V value;

  IndexElem(@Nonnull IndexKey indexKey, @Nonnull V value) {
    this.key = indexKey;
    this.value = value;
  }

  @Override
  @Nonnull
  public IndexKey key() {
    return key;
  }

  @Override
  @Nonnull
  public V value() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Index.Element<?> other) {
      return key.equals(other.key()) && value.equals(other.value());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = key.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }
}
