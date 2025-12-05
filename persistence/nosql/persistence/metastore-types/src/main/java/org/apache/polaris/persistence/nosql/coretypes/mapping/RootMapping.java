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

import jakarta.annotation.Nonnull;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.coretypes.realm.RootObj;

final class RootMapping extends BaseMapping<RootObj, RootObj.Builder> {
  RootMapping() {
    super(RootObj.TYPE, null, ROOT_REF_NAME, PolarisEntityType.ROOT);
  }

  @Override
  public RootObj.Builder newObjBuilder(@Nonnull PolarisEntitySubType subType) {
    return RootObj.builder();
  }
}
