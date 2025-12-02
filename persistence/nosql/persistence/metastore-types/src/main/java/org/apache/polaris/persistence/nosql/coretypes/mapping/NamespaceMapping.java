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

package org.apache.polaris.persistence.nosql.coretypes.mapping;

import static org.apache.polaris.core.entity.PolarisEntitySubType.NULL_SUBTYPE;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.content.LocalNamespaceObj;
import org.apache.polaris.persistence.nosql.coretypes.content.NamespaceObj;

final class NamespaceMapping<O extends NamespaceObj, B extends NamespaceObj.Builder<O, B>>
    extends BaseCatalogContentMapping<O, B> {
  NamespaceMapping() {
    super(
        NamespaceObj.class,
        Map.of(NULL_SUBTYPE, LocalNamespaceObj.TYPE),
        CatalogStateObj.TYPE,
        CATALOG_STATE_REF_NAME_PATTERN,
        PolarisEntityType.NAMESPACE);
  }

  @Override
  public B newObjBuilder(@Nonnull PolarisEntitySubType subType) {
    return cast(LocalNamespaceObj.builder());
  }
}
