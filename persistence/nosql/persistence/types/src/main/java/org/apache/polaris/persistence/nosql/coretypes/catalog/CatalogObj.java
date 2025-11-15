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
package org.apache.polaris.persistence.nosql.coretypes.catalog;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.AbstractObjType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;

@PolarisImmutable
@JsonSerialize(as = ImmutableCatalogObj.class)
@JsonDeserialize(as = ImmutableCatalogObj.class)
public interface CatalogObj extends CatalogObjBase {
  ObjType TYPE = new CatalogObjType();

  CatalogType catalogType();

  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  Optional<String> defaultBaseLocation();

  CatalogStatus status();

  @Override
  default ObjType type() {
    return TYPE;
  }

  static Builder builder() {
    return ImmutableCatalogObj.builder();
  }

  final class CatalogObjType extends AbstractObjType<CatalogObj> {
    public CatalogObjType() {
      super("cat", "Catalog", CatalogObj.class);
    }
  }

  @SuppressWarnings("unused")
  interface Builder extends CatalogObjBase.Builder<CatalogObj, Builder> {
    @CanIgnoreReturnValue
    Builder from(CatalogObj from);

    @CanIgnoreReturnValue
    Builder status(CatalogStatus status);

    @CanIgnoreReturnValue
    Builder catalogType(CatalogType catalogType);

    @CanIgnoreReturnValue
    Builder defaultBaseLocation(String defaultBaseLocation);

    @CanIgnoreReturnValue
    Builder defaultBaseLocation(Optional<String> defaultBaseLocation);
  }
}
