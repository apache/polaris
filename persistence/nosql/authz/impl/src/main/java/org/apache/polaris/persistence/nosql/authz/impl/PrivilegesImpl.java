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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static org.agrona.collections.ObjectHashSet.DEFAULT_INITIAL_CAPACITY;
import static org.apache.polaris.persistence.nosql.authz.api.PredefinedRoles.ANONYMOUS_ROLE;
import static org.apache.polaris.persistence.nosql.authz.api.PredefinedRoles.PUBLIC_ROLE;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.authz.api.Acl;
import org.apache.polaris.persistence.nosql.authz.api.AclEntry;
import org.apache.polaris.persistence.nosql.authz.api.Privilege;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeCheck;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeSet;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;
import org.apache.polaris.persistence.nosql.authz.spi.ImmutablePrivilegesMapping;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegeDefinition;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegesProvider;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegesRepository;
import org.immutables.value.Value;

@ApplicationScoped
class PrivilegesImpl implements Privileges {
  private final Object2ObjectHashMap<String, PrivilegeAndId> nameToPrivilege;
  private final Int2ObjectHashMap<PrivilegeAndId> idToPrivilege;
  private final PrivilegeSet nonInheritablePrivileges;
  private final Privilege.CompositePrivilege[] compositePrivileges;
  private final Collection<Privilege> allPrivileges;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  PrivilegesImpl(
      Instance<PrivilegesProvider> privilegesProviders, PrivilegesRepository privilegesRepository) {
    this(privilegesProviders.stream(), privilegesRepository);
  }

  @VisibleForTesting
  PrivilegesImpl(Stream<PrivilegesProvider> stream, PrivilegesRepository privilegesRepository) {
    var providedPrivileges =
        new HashMap<String, Map.Entry<PrivilegeDefinition, PrivilegesProvider>>();
    for (var providerIter = stream.iterator(); providerIter.hasNext(); ) {
      var privilegesProvider = providerIter.next();
      for (var definitions = privilegesProvider.privilegeDefinitions().iterator();
          definitions.hasNext(); ) {
        var definition = definitions.next();
        var duplicate =
            providedPrivileges.putIfAbsent(
                definition.privilege().name(), Map.entry(definition, privilegesProvider));
        if (duplicate != null) {
          throw new IllegalStateException(
              format(
                  "Duplicate privilege definition for name '%s'", definition.privilege().name()));
        }
      }
    }

    var individualPrivileges =
        providedPrivileges.values().stream()
            .map(Map.Entry::getKey)
            .map(PrivilegeDefinition::privilege)
            .filter(Privilege.IndividualPrivilege.class::isInstance)
            .toList();
    var individualPrivilegeNames =
        individualPrivileges.stream().map(Privilege::name).collect(Collectors.toSet());

    while (true) {
      var mapping = privilegesRepository.fetchPrivilegesMapping();
      var mapped = mapping.nameToId();
      var maxId = mapped.values().stream().max(Integer::compareTo).orElse(-1);

      if (!mapped.keySet().containsAll(individualPrivilegeNames)) {
        // not all individual privileges have an integer ID - need to persist an updated version of
        // the privilege name-to-id mapping!

        var existingNames = mapped.keySet();
        var namesToMap = new HashSet<>(individualPrivilegeNames);
        namesToMap.removeAll(existingNames);

        var newMappingBuilder = ImmutablePrivilegesMapping.builder();
        newMappingBuilder.putAllNameToId(mapped);
        for (var nameToMap : namesToMap) {
          newMappingBuilder.putNameToId(nameToMap, ++maxId);
        }

        var newMapping = newMappingBuilder.build();
        if (privilegesRepository.updatePrivilegesMapping(mapping, newMapping)) {
          // our update worked, go ahead
          mapped = newMapping.nameToId();
        } else {
          // oops, a race with a concurrently starting instance, retry...
          continue;
        }
      }

      // At this point we know that all individual privileges have a valid and persisted
      // constant integer ID
      this.nameToPrivilege =
          new Object2ObjectHashMap<>(DEFAULT_INITIAL_CAPACITY, Hashing.DEFAULT_LOAD_FACTOR, false);
      this.idToPrivilege =
          new Int2ObjectHashMap<>(DEFAULT_INITIAL_CAPACITY, Hashing.DEFAULT_LOAD_FACTOR, false);
      var nonInheritablePrivilegesBuilder = PrivilegeSetImpl.builder(this);
      var compositePrivilegesBuilder = new ArrayList<Privilege.CompositePrivilege>();
      for (var provided : providedPrivileges.values()) {
        var privilege = provided.getKey().privilege();
        var name = privilege.name();
        var id = (int) requireNonNull(mapped.getOrDefault(name, -1));
        this.nameToPrivilege.put(name, ImmutablePrivilegeAndId.of(privilege, id));
        if (id != -1) {
          this.idToPrivilege.put(id, ImmutablePrivilegeAndId.of(privilege, id));
        }
        if (privilege instanceof Privilege.NonInheritablePrivilege nonInheritablePrivilege) {
          nonInheritablePrivilegesBuilder.addPrivilege(nonInheritablePrivilege);
        } else if (privilege instanceof Privilege.CompositePrivilege compositePrivilege) {
          compositePrivilegesBuilder.add(compositePrivilege);
        }
      }
      this.nonInheritablePrivileges = nonInheritablePrivilegesBuilder.build();
      this.compositePrivileges =
          compositePrivilegesBuilder.toArray(Privilege.CompositePrivilege[]::new);
      this.allPrivileges =
          nameToPrivilege.values().stream().map(PrivilegeAndId::privilege).toList();

      break;
    }
  }

  @PolarisImmutable
  interface PrivilegeAndId {
    @Value.Parameter
    Privilege privilege();

    @Value.Parameter
    int id();
  }

  @Override
  public Privilege byName(@Nonnull String name) {
    var ex = nameToPrivilege.get(name);
    checkArgument(ex != null, "Unknown privilege '%s'", name);
    return ex.privilege();
  }

  @Override
  public Privilege byId(int id) {
    var ex = idToPrivilege.get(id);
    checkArgument(ex != null, "Unknown privilege ID %s", id);
    return ex.privilege();
  }

  @Override
  public int idForName(@Nonnull String name) {
    var ex = nameToPrivilege.get(name);
    checkArgument(ex != null && ex.id() >= 0, "Unknown individual privilege '%s'", name);
    return ex.id();
  }

  @Override
  public int idForPrivilege(@Nonnull Privilege privilege) {
    return idForName(privilege.name());
  }

  @Override
  public Set<String> allNames() {
    return unmodifiableSet(nameToPrivilege.keySet());
  }

  @Override
  public Set<Integer> allIds() {
    return unmodifiableSet(idToPrivilege.keySet());
  }

  @Override
  public PrivilegeSet nonInheritablePrivileges() {
    return nonInheritablePrivileges;
  }

  @Override
  public Set<Privilege> collapseComposites(@Nonnull PrivilegeSet value) {
    Set<Privilege> collapsed = new HashSet<>();

    var work = newPrivilegesSetBuilder().addPrivileges(value);
    for (Privilege.CompositePrivilege compositePrivilege : compositePrivileges) {
      if (value.contains(compositePrivilege)) {
        work.removePrivileges(compositePrivilege);
        collapsed.add(compositePrivilege);
      }
    }

    collapsed.addAll(work.build());

    return collapsed;
  }

  @Override
  public Collection<Privilege> all() {
    return allPrivileges;
  }

  @Override
  public PrivilegeSet.PrivilegeSetBuilder newPrivilegesSetBuilder() {
    return PrivilegeSetImpl.builder(this);
  }

  @Override
  public AclEntry.AclEntryBuilder newAclEntryBuilder() {
    return new AclEntryBuilderImpl(this);
  }

  @Override
  public Acl.AclBuilder newAclBuilder() {
    return AclImpl.builder(this);
  }

  @Override
  public PrivilegeCheck startPrivilegeCheck(boolean anonymous, Set<String> roleIds) {
    Set<String> effectiveRoles = new HashSet<>(roleIds);
    effectiveRoles.add(anonymous ? ANONYMOUS_ROLE : PUBLIC_ROLE);
    return new PrivilegeCheckImpl(effectiveRoles, this);
  }
}
