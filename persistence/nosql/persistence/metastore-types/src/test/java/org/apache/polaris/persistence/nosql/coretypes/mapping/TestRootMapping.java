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

import static org.apache.polaris.persistence.nosql.coretypes.realm.RootObj.ROOT_REF_NAME;

import java.util.stream.Stream;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.obj.ObjType;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.realm.RootObj;
import org.junit.jupiter.params.provider.MethodSource;

@MethodSource("parameters")
public class TestRootMapping extends BaseTestMapping {
  @Override
  public ObjType containerObjType() {
    return null;
  }

  @Override
  public Class<? extends ObjBase> baseObjClass() {
    return null;
  }

  @Override
  public String refName() {
    return ROOT_REF_NAME;
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
    return false;
  }

  static Stream<BaseTestParameter> parameters() {
    return Stream.of(
        new BaseTestParameter(
            PolarisEntityType.ROOT, PolarisEntitySubType.NULL_SUBTYPE, RootObj.TYPE) {
          @Override
          public ObjBase.Builder<?, ?> objBuilder() {
            return RootObj.builder();
          }
        });
  }
}
