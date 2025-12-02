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

import static org.apache.polaris.core.entity.CatalogEntity.CATALOG_TYPE_PROPERTY;
import static org.apache.polaris.core.entity.CatalogEntity.DEFAULT_BASE_LOCATION_KEY;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj.CATALOGS_REF_NAME;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithInternalProperty;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithProperty;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogObj;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStatus;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogType;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogsObj;
import org.junit.jupiter.params.provider.MethodSource;

@MethodSource("parameters")
public class TestCatalogMapping extends BaseTestMapping {
  @Override
  public ObjType containerObjType() {
    return CatalogsObj.TYPE;
  }

  @Override
  public Class<? extends ObjBase> baseObjClass() {
    return null;
  }

  @Override
  public String refName() {
    return CATALOGS_REF_NAME;
  }

  @Override
  public boolean isCatalogContent() {
    return false;
  }

  @Override
  public boolean isCatalogRelated() {
    return false;
  }

  @Override
  public boolean isWithStorage() {
    return true;
  }

  static Stream<BaseTestParameter> parameters() {
    return Stream.of(
        new BaseTestParameter(
            PolarisEntityType.CATALOG, PolarisEntitySubType.NULL_SUBTYPE, CatalogObj.TYPE) {
          @Override
          public CatalogObj.Builder objBuilder() {
            return CatalogObj.builder()
                .catalogType(CatalogType.INTERNAL)
                .status(CatalogStatus.ACTIVE);
          }

          @Override
          public PolarisBaseEntity.Builder entityBuilder() {
            return super.entityBuilder()
                .internalPropertiesAsMap(Map.of(CATALOG_TYPE_PROPERTY, "INTERNAL"));
          }

          @Override
          public Stream<MappingSample> typeVariations(MappingSample base) {
            return storageVariations(this::objBuilder, base)
                .flatMap(
                    b ->
                        Stream.of(
                            b,
                            //
                            new MappingSample(
                                objBuilder()
                                    .from(b.obj())
                                    .catalogType(CatalogType.EXTERNAL)
                                    .build(),
                                entityWithInternalProperty(
                                    b.entity(), CATALOG_TYPE_PROPERTY, "EXTERNAL")),
                            //
                            new MappingSample(
                                objBuilder()
                                    .from(b.obj())
                                    .defaultBaseLocation("s3://foo/bar/baz/")
                                    .build(),
                                entityWithProperty(
                                    b.entity(), DEFAULT_BASE_LOCATION_KEY, "s3://foo/bar/baz/")),
                            //
                            new MappingSample(
                                objBuilder()
                                    .from(b.obj())
                                    .catalogType(CatalogType.EXTERNAL)
                                    .defaultBaseLocation("s3://foo/bar/baz/")
                                    .build(),
                                entityWithInternalProperty(
                                    entityWithProperty(
                                        b.entity(), DEFAULT_BASE_LOCATION_KEY, "s3://foo/bar/baz/"),
                                    CATALOG_TYPE_PROPERTY,
                                    "EXTERNAL"))
                            //
                            ));
          }
        });
  }
}
