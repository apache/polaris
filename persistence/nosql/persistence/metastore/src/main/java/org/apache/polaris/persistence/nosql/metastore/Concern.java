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
package org.apache.polaris.persistence.nosql.metastore;

import static com.google.common.base.Preconditions.checkArgument;

import jakarta.annotation.Nonnull;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityType;

record Concern(@Nonnull PolarisEntityType entityType, long catalogId, boolean catalogContent) {
  static Concern forEntity(PolarisEntityCore entity) {
    var type = entity.getType();
    var catalogId = entity.getCatalogId();
    var catContent = TypeMapping.isCatalogContent(type);
    checkArgument(
        (!catContent && type != PolarisEntityType.CATALOG_ROLE) || catalogId > 0L,
        "catalogId must be a positive integer for %s",
        type);
    return new Concern(type, catalogId, catContent);
  }
}
