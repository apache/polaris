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

import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN;

import java.util.stream.Stream;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.content.LocalNamespaceObj;
import org.apache.polaris.persistence.nosql.coretypes.content.NamespaceObj;
import org.junit.jupiter.params.provider.MethodSource;

@MethodSource("parameters")
public class TestNamespaceMapping extends BaseTestMapping {
  @Override
  public ObjType containerObjType() {
    return CatalogStateObj.TYPE;
  }

  @Override
  public Class<? extends ObjBase> baseObjClass() {
    return NamespaceObj.class;
  }

  @Override
  public String refName() {
    return CATALOG_STATE_REF_NAME_PATTERN;
  }

  @Override
  public boolean isCatalogContent() {
    return true;
  }

  @Override
  public boolean isCatalogRelated() {
    return true;
  }

  @Override
  public boolean isWithStorage() {
    return false;
  }

  static Stream<BaseTestParameter> parameters() {
    return Stream.of(
        new BaseTestParameter(
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.NULL_SUBTYPE,
            LocalNamespaceObj.TYPE) {
          @Override
          public ObjBase.Builder<?, ?> objBuilder() {
            return LocalNamespaceObj.builder();
          }

          @Override
          public PolarisBaseEntity.Builder entityBuilder() {
            return super.entityBuilder().catalogId(123L);
          }
        });
  }
}
