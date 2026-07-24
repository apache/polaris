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

package org.apache.polaris.core.collection;

/**
 * A mutable, type-safe {@link AttributeMap}. Mutable attribute maps are not thread-safe and may
 * throw {@link java.util.ConcurrentModificationException} if modified by two threads concurrently.
 */
public final class MutableAttributeMap extends AbstractAttributeMap implements AttributeMap {

  /** Fluent builder for {@link MutableAttributeMap}. */
  public static class Builder
      extends AbstractAttributeMap.Builder<MutableAttributeMap, MutableAttributeMap.Builder> {

    @Override
    protected MutableAttributeMap.Builder self() {
      return this;
    }

    @Override
    public MutableAttributeMap build() {
      return map;
    }
  }

  /** Returns a new, empty {@link Builder}. */
  public static Builder builder() {
    return new Builder();
  }

  /** Creates an empty mutable map. */
  public MutableAttributeMap() {
    super();
  }

  /** Copy constructor. */
  public MutableAttributeMap(AttributeMap toCopy) {
    super(toCopy);
  }

  public ImmutableAttributeMap toImmutableAttributeMap() {
    return new ImmutableAttributeMap(this);
  }
}
