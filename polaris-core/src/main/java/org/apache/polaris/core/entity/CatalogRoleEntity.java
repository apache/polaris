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
package org.apache.polaris.core.entity;

import org.apache.polaris.core.admin.model.CatalogRole;

/** Wrapper for translating between the REST CatalogRole object and the base PolarisEntity type. */
public class CatalogRoleEntity extends PolarisEntity {
  public CatalogRoleEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static CatalogRoleEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new CatalogRoleEntity(sourceEntity);
    }
    return null;
  }

  public static CatalogRoleEntity fromCatalogRole(CatalogRole catalogRole) {
    return new Builder()
        .setName(catalogRole.getName())
        .setProperties(catalogRole.getProperties())
        .build();
  }

  public static class Builder extends PolarisEntity.BaseBuilder<CatalogRoleEntity, Builder> {
    public Builder() {
      super();
      setType(PolarisEntityType.CATALOG_ROLE);
    }

    public Builder(CatalogRoleEntity original) {
      super(original);
    }

    @Override
    public CatalogRoleEntity build() {
      return new CatalogRoleEntity(buildBase());
    }
  }
}
