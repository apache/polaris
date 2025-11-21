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

import java.util.function.Supplier;

/**
 * This is an <em>internal</em> utility class, which provides a non-synchronized, not thread-safe
 * {@link Supplier} that memoizes returned value but also a thrown exception.
 */
final class SupplyOnce {

  private SupplyOnce() {}

  static <T> Supplier<T> memoize(Supplier<T> loader) {
    return new NonLockingSupplyOnce<>(loader);
  }

  private static final class NonLockingSupplyOnce<T> implements Supplier<T> {
    private int loaded;
    private Object result;
    private final Supplier<T> loader;

    private NonLockingSupplyOnce(Supplier<T> loader) {
      this.loader = loader;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get() {
      return switch (loaded) {
        case 1 -> (T) result;
        case 2 -> throw (RuntimeException) result;
        case 0 -> load();
        default -> throw new IllegalStateException();
      };
    }

    private T load() {
      try {
        loaded = 1;
        T obj = loader.get();
        result = obj;
        return obj;
      } catch (RuntimeException re) {
        loaded = 2;
        result = re;
        throw re;
      }
    }
  }
}
