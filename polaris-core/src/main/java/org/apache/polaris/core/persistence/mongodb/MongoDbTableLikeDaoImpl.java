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

package org.apache.polaris.core.persistence.mongodb;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.dao.TableLikeDao;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public class MongoDbTableLikeDaoImpl implements TableLikeDao {
    @Override
    public @NotNull EntityResult readEntityByName(@NotNull PolarisCallContext callCtx, @Nullable List<PolarisEntityCore> catalogPath, @NotNull PolarisEntitySubType entitySubType, @NotNull String name) {
        return null;
    }

    @NotNull
    @Override
    public ListEntitiesResult listEntities(@NotNull PolarisCallContext callCtx, @NotNull List<PolarisEntityCore> catalogPath, @NotNull PolarisEntitySubType entitySubType) {
        return null;
    }
}
