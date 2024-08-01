/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;

/**
 * Namespace-specific subclass of the {@link PolarisEntity} that provides accessors interacting with
 * internalProperties specific to the NAMESPACE type.
 */
public class NamespaceEntity extends PolarisEntity {
  // RESTUtil-encoded parent namespace.
  public static final String PARENT_NAMESPACE_KEY = "parent-namespace";

  public NamespaceEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static NamespaceEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new NamespaceEntity(sourceEntity);
    }
    return null;
  }

  public Namespace getParentNamespace() {
    String encodedNamespace = getInternalPropertiesAsMap().get(PARENT_NAMESPACE_KEY);
    if (encodedNamespace == null) {
      return Namespace.empty();
    }
    return RESTUtil.decodeNamespace(encodedNamespace);
  }

  public Namespace asNamespace() {
    Namespace parent = getParentNamespace();
    String[] levels = new String[parent.length() + 1];
    for (int i = 0; i < parent.length(); ++i) {
      levels[i] = parent.level(i);
    }
    levels[levels.length - 1] = getName();
    return Namespace.of(levels);
  }

  @JsonIgnore
  public String getBaseLocation() {
    return getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
  }

  public static class Builder extends PolarisEntity.BaseBuilder<NamespaceEntity, Builder> {
    public Builder(Namespace namespace) {
      super();
      setType(PolarisEntityType.NAMESPACE);
      setParentNamespace(PolarisCatalogHelpers.getParentNamespace(namespace));
      setName(namespace.level(namespace.length() - 1));
    }

    private Builder setBaseLocation(String baseLocation) {
      properties.put(PolarisEntityConstants.ENTITY_BASE_LOCATION, baseLocation);
      return this;
    }

    public Builder setParentNamespace(Namespace namespace) {
      if (namespace != null && !namespace.isEmpty()) {
        internalProperties.put(PARENT_NAMESPACE_KEY, RESTUtil.encodeNamespace(namespace));
      }
      return this;
    }

    public Builder setLocation(String location) {
      super.setLocation(location);
      setBaseLocation(location);
      return this;
    }

    public NamespaceEntity build() {
      return new NamespaceEntity(buildBase());
    }
  }
}
