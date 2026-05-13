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

package org.apache.polaris.service.events.listeners;

import io.quarkus.arc.DefaultBean;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.service.events.AttributeKey;
import org.apache.polaris.service.events.EventAttributeFilter;
import org.apache.polaris.service.events.EventAttributes;

@ApplicationScoped
@DefaultBean
class DefaultEventAttributeFilter implements EventAttributeFilter {

  private static final Set<AttributeKey<?>> CONFIGURATION_DENYLIST =
      Set.of(
          EventAttributes.PRINCIPAL,
          EventAttributes.UPDATE_PRINCIPAL_REQUEST,
          EventAttributes.CATALOG,
          EventAttributes.NOTIFICATION_REQUEST);

  static final Set<AttributeKey<?>> DEFAULT_ALLOWLIST =
      Set.of(
          EventAttributes.CATALOG_NAME,
          EventAttributes.NAMESPACE,
          EventAttributes.NAMESPACE_FQN,
          EventAttributes.PARENT_NAMESPACE_FQN,
          EventAttributes.TABLE_NAME,
          EventAttributes.TABLE_IDENTIFIER,
          EventAttributes.TABLE_METADATA,
          EventAttributes.LOAD_TABLE_RESPONSE,
          EventAttributes.RENAME_TABLE_REQUEST,
          EventAttributes.PURGE_REQUESTED,
          EventAttributes.VIEW_NAME,
          EventAttributes.VIEW_IDENTIFIER,
          EventAttributes.NAMESPACE_NAME,
          EventAttributes.GENERIC_TABLE_NAME);

  @Inject PolarisPersistenceEventListenerConfiguration configuration;

  private Set<AttributeKey<?>> allowlist;

  DefaultEventAttributeFilter() {
    this(DEFAULT_ALLOWLIST);
  }

  DefaultEventAttributeFilter(Set<AttributeKey<?>> allowlist) {
    this.allowlist = validateAllowlist(allowlist);
  }

  @PostConstruct
  void initializeFromConfiguration() {
    if (configuration == null) {
      return;
    }
    Set<String> configuredNames =
        configuration.allowlistedAttributes().orElse(Collections.emptySet());
    if (!configuredNames.isEmpty()) {
      this.allowlist = resolveConfiguredAllowlist(configuredNames);
    }
  }

  @Override
  public boolean isAllowed(AttributeKey<?> key) {
    return allowlist.contains(key);
  }

  static Set<AttributeKey<?>> resolveConfiguredAllowlist(Set<String> configuredNames) {
    if (configuredNames.isEmpty()) {
      return DEFAULT_ALLOWLIST;
    }

    Set<AttributeKey<?>> resolved = new LinkedHashSet<>();
    List<String> unknown = new ArrayList<>();

    for (String name : configuredNames) {
      EventAttributes.findByName(name).ifPresentOrElse(resolved::add, () -> unknown.add(name));
    }

    if (!unknown.isEmpty()) {
      throw new IllegalArgumentException(
          "Unknown event attributes in persistence allowlist configuration: " + unknown);
    }

    return validateAllowlist(resolved);
  }

  private static Set<AttributeKey<?>> validateAllowlist(Set<AttributeKey<?>> allowlist) {
    Set<AttributeKey<?>> denied =
        allowlist.stream()
            .filter(CONFIGURATION_DENYLIST::contains)
            .collect(Collectors.toCollection(LinkedHashSet::new));

    if (!denied.isEmpty()) {
      throw new IllegalArgumentException(
          "Sensitive attributes are not allowed in persistence allowlist configuration: "
              + denied.stream().map(AttributeKey::name).toList());
    }

    return Set.copyOf(allowlist);
  }
}
