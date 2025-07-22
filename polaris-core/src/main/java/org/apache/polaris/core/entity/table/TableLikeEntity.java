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
package org.apache.polaris.core.entity.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.annotation.Nonnull;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;

/**
 * An entity type for all table-like entities including Iceberg tables, Iceberg views, and generic
 * tables. This entity maps to {@link PolarisEntityType#TABLE_LIKE}
 */
public abstract class TableLikeEntity extends PolarisEntity implements LocationBasedEntity {

  public TableLikeEntity(@Nonnull PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  @JsonIgnore
  public TableIdentifier getTableIdentifier() {
    Namespace parent = getParentNamespace();
    return TableIdentifier.of(parent, getName());
  }

  @JsonIgnore
  public Namespace getParentNamespace() {
    String encodedNamespace =
        getInternalPropertiesAsMap().get(NamespaceEntity.PARENT_NAMESPACE_KEY);
    if (encodedNamespace == null) {
      return Namespace.empty();
    }
    return RESTUtil.decodeNamespace(encodedNamespace);
  }
}
