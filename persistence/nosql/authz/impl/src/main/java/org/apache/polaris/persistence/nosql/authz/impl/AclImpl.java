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
package org.apache.polaris.persistence.nosql.authz.impl;

import static org.agrona.collections.ObjectHashSet.DEFAULT_INITIAL_CAPACITY;

import jakarta.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.polaris.persistence.nosql.authz.api.Acl;
import org.apache.polaris.persistence.nosql.authz.api.AclEntry;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;

record AclImpl(Object2ObjectHashMap<String, AclEntry> map) implements Acl {

  AclImpl(AclBuilderImpl map) {
    this(new Object2ObjectHashMap<>(map.map));
  }

  @Override
  public void forEach(@Nonnull BiConsumer<String, AclEntry> consumer) {
    map.forEach(consumer);
  }

  @Override
  public void entriesForRoleIds(
      @Nonnull Set<String> roleIds, @Nonnull Consumer<AclEntry> aclEntryConsumer) {
    roleIds.stream().map(map::get).filter(Objects::nonNull).forEach(aclEntryConsumer);
  }

  static AclBuilder builder(Privileges privileges) {
    return new AclBuilderImpl(privileges);
  }

  @Override
  @SuppressWarnings("EqualsGetClass")
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AclImpl acl = (AclImpl) o;
    return map.equals(acl.map);
  }

  @Override
  @Nonnull
  public String toString() {
    return "Acl{"
        + map.entrySet().stream()
            .map(e -> e.getKey() + ":" + e.getValue())
            .collect(Collectors.joining(","))
        + "}";
  }

  private static final class AclBuilderImpl implements AclBuilder {

    private final Object2ObjectHashMap<String, AclEntry> map;
    private final Privileges privileges;

    private AclBuilderImpl(Privileges privileges) {
      this.privileges = privileges;
      this.map =
          new Object2ObjectHashMap<>(DEFAULT_INITIAL_CAPACITY, Hashing.DEFAULT_LOAD_FACTOR, false);
    }

    @Override
    public AclBuilder from(@Nonnull Acl instance) {
      map.clear();
      map.putAll(((AclImpl) instance).map);
      return this;
    }

    @Override
    public AclBuilder addEntry(@Nonnull String roleId, @Nonnull AclEntry entry) {
      map.put(roleId, entry);
      return this;
    }

    @Override
    public AclBuilder removeEntry(@Nonnull String roleId) {
      map.remove(roleId);
      return this;
    }

    @Override
    public AclBuilder modify(
        @Nonnull String roleId, @Nonnull Consumer<AclEntry.AclEntryBuilder> entry) {
      map.compute(
          roleId,
          (k, e) -> {
            AclEntry.AclEntryBuilder builder =
                e != null
                    ? new AclEntryBuilderImpl(privileges, e)
                    : new AclEntryBuilderImpl(privileges);
            entry.accept(builder);
            AclEntry updated = builder.build();
            return updated.isEmpty() ? null : updated;
          });
      return this;
    }

    @Override
    public Acl build() {
      return new AclImpl(this);
    }
  }
}
