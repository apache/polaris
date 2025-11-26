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
package org.apache.polaris.persistence.nosql.maintenance.spi;

import java.util.function.Predicate;

/**
 * Predicate that yields {@code true} for the number of {@link #test(Object)} invocations given to
 * the constructor.
 */
public final class CountDownPredicate<T> implements Predicate<T> {
  private int remaining;

  public CountDownPredicate(int remaining) {
    if (remaining <= 0) {
      throw new IllegalArgumentException("remaining must be > 0");
    }
    this.remaining = remaining;
  }

  @Override
  public boolean test(T t) {
    if (remaining <= 0) {
      return false;
    }
    remaining--;
    return true;
  }
}
