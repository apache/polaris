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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.errorprone.annotations.Var;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Objects;

/**
 * Base class for {@link Index.Element} implementations that provides coherent {@link
 * #equals(Object)} and {@link #hashCode()} semantics.
 */
public abstract class IndexElem<V> implements Index.Element<V> {

  public static <V> Index.Element<V> of(@Nonnull IndexKey key, @Nonnull V value) {
    return new IndexElem<>() {
      @Override
      @Nonnull
      public IndexKey key() {
        return key;
      }

      @Override
      @Nonnull
      public V valueNullable() {
        return value;
      }
    };
  }

  @Nullable
  protected abstract V valueNullable();

  @Override
  @Nonnull
  public final V value() {
    return checkNotNull(valueNullable(), key());
  }

  @Override
  public int hashCode() {
    @Var var h = 5381;
    h += (h << 5) + key().hashCode();
    V value = valueNullable();
    if (value != null) {
      h += (h << 5) + value.hashCode();
    }
    return h;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    // This equals method considers other implementations of Index.Element as "not equal"
    // because it is impossible to guarantee that their hashCode() is equal to the hash
    // code produced by this class (when equal() == true) without knowing the specific
    // "other" implementation class. All Index.Element inside Polaris code are expected
    // to extend this class.
    if (o instanceof IndexElem<?> other) {
      return Objects.equals(key(), other.key())
          && Objects.equals(valueNullable(), other.valueNullable());
    }
    return false;
  }
}
