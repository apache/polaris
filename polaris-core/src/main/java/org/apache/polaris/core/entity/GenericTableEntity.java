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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;

/**
 * A {@link PolarisEntity} implementation for generic tables. These tables are not Iceberg-like in
 * that they may not have a schema or base location. Similarly to {@link IcebergTableLikeEntity}
 * however, these tables have an identifier and a parent namespace.
 */
public class GenericTableEntity extends PolarisEntity {

  public static final String FORMAT_KEY = "format";

  public GenericTableEntity(PolarisBaseEntity sourceEntity) {
    super(sourceEntity);
  }

  public static GenericTableEntity of(PolarisBaseEntity sourceEntity) {
    if (sourceEntity != null) {
      return new GenericTableEntity(sourceEntity);
    }
    return null;
  }

  @JsonIgnore
  public String getFormat() {
    return getPropertiesAsMap().get(GenericTableEntity.FORMAT_KEY);
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

  public static class Builder
      extends PolarisEntity.BaseBuilder<GenericTableEntity, GenericTableEntity.Builder> {
    public Builder(String format) {
      super();
      setType(PolarisEntityType.GENERIC_TABLE);
      setFormat(format);
    }

    public GenericTableEntity.Builder setFormat(String format) {
      // TODO in the future, we may validate the format and require certain properties
      properties.put(GenericTableEntity.FORMAT_KEY, format);
      return this;
    }

    public GenericTableEntity.Builder setTableIdentifier(TableIdentifier identifier) {
      Namespace namespace = identifier.namespace();
      setParentNamespace(namespace);
      setName(identifier.name());
      return this;
    }

    public GenericTableEntity.Builder setParentNamespace(Namespace namespace) {
      if (namespace != null && !namespace.isEmpty()) {
        internalProperties.put(
            NamespaceEntity.PARENT_NAMESPACE_KEY, RESTUtil.encodeNamespace(namespace));
      }
      return this;
    }

    @Override
    public GenericTableEntity build() {
      return new GenericTableEntity(buildBase());
    }
  }
}
