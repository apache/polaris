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
package org.apache.polaris.persistence.nosql.authz.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * An {@link Acl ACL} entry holds the {@linkplain PrivilegeSet sets} of <em>granted</em> and
 * <em>restricted</em> ("separation of duties") {@linkplain Privilege privileges}.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableAclEntry.class)
@JsonDeserialize(as = ImmutableAclEntry.class)
public interface AclEntry {
  @Value.Parameter(order = 1)
  @Value.Default
  // The 'CUSTOM/valueFilter' combination is there to only include non-empty privilege sets
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = PrivilegeSetJsonFilter.class)
  default PrivilegeSet granted() {
    return PrivilegeSet.emptyPrivilegeSet();
  }

  @Value.Parameter(order = 2)
  @Value.Default
  // The 'CUSTOM/valueFilter' combination is there to only include non-empty privilege sets
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = PrivilegeSetJsonFilter.class)
  default PrivilegeSet restricted() {
    return PrivilegeSet.emptyPrivilegeSet();
  }

  @Value.NonAttribute
  @JsonIgnore
  default boolean isEmpty() {
    return granted().isEmpty() && restricted().isEmpty();
  }

  interface AclEntryBuilder {
    @CanIgnoreReturnValue
    AclEntryBuilder grant(@Nonnull Collection<? extends Privilege> privileges);

    @CanIgnoreReturnValue
    AclEntryBuilder grant(@Nonnull Privilege... privileges);

    @CanIgnoreReturnValue
    AclEntryBuilder grant(@Nonnull Privilege privilege);

    @CanIgnoreReturnValue
    AclEntryBuilder grant(@Nonnull PrivilegeSet privileges);

    @CanIgnoreReturnValue
    AclEntryBuilder revoke(@Nonnull Collection<? extends Privilege> privileges);

    @CanIgnoreReturnValue
    AclEntryBuilder revoke(@Nonnull Privilege... privileges);

    @CanIgnoreReturnValue
    AclEntryBuilder revoke(@Nonnull Privilege privilege);

    @CanIgnoreReturnValue
    AclEntryBuilder revoke(@Nonnull PrivilegeSet privileges);

    @CanIgnoreReturnValue
    AclEntryBuilder restrict(@Nonnull Collection<? extends Privilege> privileges);

    @CanIgnoreReturnValue
    AclEntryBuilder restrict(@Nonnull Privilege... privileges);

    @CanIgnoreReturnValue
    AclEntryBuilder restrict(@Nonnull Privilege privilege);

    @CanIgnoreReturnValue
    AclEntryBuilder restrict(@Nonnull PrivilegeSet privileges);

    @CanIgnoreReturnValue
    AclEntryBuilder unrestrict(@Nonnull Collection<? extends Privilege> privileges);

    @CanIgnoreReturnValue
    AclEntryBuilder unrestrict(@Nonnull Privilege... privileges);

    @CanIgnoreReturnValue
    AclEntryBuilder unrestrict(@Nonnull Privilege privilege);

    @CanIgnoreReturnValue
    AclEntryBuilder unrestrict(@Nonnull PrivilegeSet privileges);

    AclEntry build();
  }
}
