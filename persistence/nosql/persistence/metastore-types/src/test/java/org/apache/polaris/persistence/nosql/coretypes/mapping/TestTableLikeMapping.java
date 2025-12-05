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

import static org.apache.polaris.core.entity.table.GenericTableEntity.DOC_KEY;
import static org.apache.polaris.core.entity.table.GenericTableEntity.FORMAT_KEY;
import static org.apache.polaris.core.entity.table.IcebergTableLikeEntity.METADATA_LOCATION_KEY;
import static org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj.CATALOG_STATE_REF_NAME_PATTERN;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithInternalProperties;
import static org.apache.polaris.persistence.nosql.coretypes.mapping.BaseTestMapping.MappingSample.entityWithInternalProperty;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.catalog.CatalogStateObj;
import org.apache.polaris.persistence.nosql.coretypes.content.GenericTableObj;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergTableObj;
import org.apache.polaris.persistence.nosql.coretypes.content.IcebergViewObj;
import org.apache.polaris.persistence.nosql.coretypes.content.TableLikeObj;
import org.junit.jupiter.params.provider.MethodSource;

@MethodSource("parameters")
public class TestTableLikeMapping extends BaseTestMapping {

  @Override
  public ObjType containerObjType() {
    return CatalogStateObj.TYPE;
  }

  @Override
  public Class<? extends ObjBase> baseObjClass() {
    return TableLikeObj.class;
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

  abstract static class TableLikeParameter extends BaseTestParameter {
    TableLikeParameter(PolarisEntitySubType subType, ObjType objType) {
      super(PolarisEntityType.TABLE_LIKE, subType, objType);
    }

    @Override
    public PolarisBaseEntity.Builder entityBuilder() {
      return super.entityBuilder().catalogId(123L);
    }
  }

  // TODO PARENT_NAMESPACE_KEY

  static Stream<BaseTestParameter> parameters() {
    return Stream.of(
        new TableLikeParameter(PolarisEntitySubType.ICEBERG_TABLE, IcebergTableObj.TYPE) {
          @Override
          public IcebergTableObj.Builder objBuilder() {
            return IcebergTableObj.builder();
          }

          @Override
          public Stream<MappingSample> typeVariations(MappingSample base) {
            return storageVariations(this::objBuilder, base);
          }
        },
        new TableLikeParameter(PolarisEntitySubType.ICEBERG_VIEW, IcebergViewObj.TYPE) {
          @Override
          public IcebergViewObj.Builder objBuilder() {
            return IcebergViewObj.builder();
          }

          @Override
          public Stream<MappingSample> typeVariations(MappingSample base) {
            return storageVariations(this::objBuilder, base);
          }
        },
        new TableLikeParameter(PolarisEntitySubType.GENERIC_TABLE, GenericTableObj.TYPE) {
          @Override
          public GenericTableObj.Builder objBuilder() {
            return GenericTableObj.builder();
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
                                    .metadataLocation("s3://foo/bar/here.json")
                                    .build(),
                                entityWithInternalProperty(
                                    b.entity(), METADATA_LOCATION_KEY, "s3://foo/bar/here.json")),
                            new MappingSample(
                                objBuilder().from(b.obj()).doc("doc").build(),
                                entityWithInternalProperty(b.entity(), DOC_KEY, "doc")),
                            new MappingSample(
                                objBuilder().from(b.obj()).format("format").build(),
                                entityWithInternalProperty(b.entity(), FORMAT_KEY, "format")),
                            //
                            new MappingSample(
                                objBuilder()
                                    .from(b.obj())
                                    .metadataLocation("s3://foo/bar/here.json")
                                    .doc("doc")
                                    .format("format")
                                    .build(),
                                entityWithInternalProperties(
                                    b.entity(),
                                    Map.of(
                                        METADATA_LOCATION_KEY,
                                        "s3://foo/bar/here.json",
                                        DOC_KEY,
                                        "doc",
                                        FORMAT_KEY,
                                        "format")))));
          }
        });
  }
}
