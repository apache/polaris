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

package org.apache.polaris.persistence.nosql.metastore.indexaccess;

import java.util.Optional;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;

final class IndexedContainerAccessEmpty<C extends ContainerObj> extends IndexedContainerAccess<C> {
  IndexedContainerAccessEmpty(Persistence persistence) {
    super(persistence);
  }

  @Override
  public Optional<C> refObj() {
    return Optional.empty();
  }

  @Override
  public Optional<ObjBase> byId(long stableId) {
    return Optional.empty();
  }

  @Override
  public Optional<IndexKey> nameKeyById(long stableId) {
    return Optional.empty();
  }

  @Override
  public Optional<ObjBase> byParentIdAndName(long parentId, String name) {
    return Optional.empty();
  }

  @Override
  public Optional<ObjBase> byNameOnRoot(String name) {
    return Optional.empty();
  }

  @Override
  public Optional<Index<ObjRef>> nameIndex() {
    return Optional.empty();
  }

  @Override
  public Optional<Index<IndexKey>> stableIdIndex() {
    return Optional.empty();
  }

  @Override
  public long catalogStableId() {
    return 0;
  }
}
