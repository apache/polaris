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

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.INDEX_KEY_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.metastore.ContentIdentifier.indexKeyToIdentifierBuilder;

import com.google.common.base.Suppliers;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.coretypes.ContainerObj;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class IndexedContainerAccessImpl<C extends ContainerObj> extends IndexedContainerAccess<C> {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexedContainerAccessImpl.class);

  private final ObjRef containerObjRef;
  private final String referenceName;
  private final Class<C> referenceObjType;
  private final long catalogStableId;
  private Optional<C> refObj;
  private final Supplier<Optional<Index<ObjRef>>> nameIndexSupplier =
      Suppliers.memoize(this::supplyNameIndex);
  private final Supplier<Optional<Index<IndexKey>>> idIndexSupplier =
      Suppliers.memoize(this::supplyIdIndex);
  private final Persistence persistence;

  IndexedContainerAccessImpl(
      Persistence persistence,
      String referenceName,
      Class<C> referenceObjType,
      long catalogStableId) {
    this.persistence = persistence;
    this.referenceName = referenceName;
    this.referenceObjType = referenceObjType;
    this.catalogStableId = catalogStableId;
    this.containerObjRef = null;
  }

  IndexedContainerAccessImpl(
      Persistence persistence, Class<C> containerObjClass, ObjRef containerObjId) {
    this.persistence = persistence;
    this.referenceName = null;
    this.referenceObjType = containerObjClass;
    this.containerObjRef = containerObjId;
    this.catalogStableId = -1;
  }

  @SuppressWarnings("OptionalAssignedToNull")
  @Override
  public boolean isStale() {
    var r = refObj;
    if (r == null || referenceName == null) {
      return false;
    }
    var current = persistence.fetchReference(referenceName);
    return !r.map(ContainerObj::id).equals(current.pointer().map(ObjRef::id));
  }

  @SuppressWarnings("OptionalAssignedToNull")
  @Override
  public Optional<C> refObj() {
    if (this.refObj == null) {
      if (referenceName != null) {
        this.refObj = persistence.fetchReferenceHead(referenceName, referenceObjType);
        LOGGER.debug("Fetched head {} for reference '{}'", refObj, referenceName);
      } else if (containerObjRef != null) {
        this.refObj = Optional.ofNullable(persistence.fetch(containerObjRef, referenceObjType));
      } else {
        // Should really never ever happen
        throw new IllegalStateException();
      }
    }
    return this.refObj;
  }

  @Override
  public Optional<ObjBase> byId(long stableId) {
    return objRefById(stableId).flatMap(this::objByRef);
  }

  @Override
  public Optional<IndexKey> nameKeyById(long stableId) {
    return stableIdIndex()
        .flatMap(
            idIndex -> {
              var nameKey = idIndex.get(IndexKey.key(stableId));
              return Optional.ofNullable(nameKey);
            });
  }

  @Override
  public Optional<ObjBase> byParentIdAndName(long parentId, String name) {
    return (parentId != 0L
            ? nameKeyById(parentId)
                .flatMap(
                    parentKey -> {
                      var fullIdentifier =
                          indexKeyToIdentifierBuilder(parentKey).addElements(name).build();
                      return objRefByName(fullIdentifier.toIndexKey());
                    })
            : objRefByName(IndexKey.key(name)))
        .flatMap(this::objByRef);
  }

  @Override
  public Optional<ObjBase> byNameOnRoot(String name) {
    return objRefByName(IndexKey.key(name)).flatMap(this::objByRef);
  }

  @Override
  public Optional<Index<ObjRef>> nameIndex() {
    return nameIndexSupplier.get();
  }

  @Override
  public Optional<Index<IndexKey>> stableIdIndex() {
    return idIndexSupplier.get();
  }

  @Override
  public long catalogStableId() {
    return catalogStableId;
  }

  private Optional<ObjRef> objRefById(long stableId) {
    return nameKeyById(stableId).flatMap(this::objRefByName);
  }

  private Optional<? extends ObjBase> objByRef(ObjRef objRef) {
    return Optional.ofNullable(persistence.fetch(objRef, ObjBase.class));
  }

  private Optional<ObjRef> objRefByName(IndexKey nameKey) {
    return nameIndex().flatMap(nameIdx -> Optional.ofNullable(nameIdx.get(nameKey)));
  }

  private Optional<Index<ObjRef>> supplyNameIndex() {
    return refObj().map(ref -> ref.nameToObjRef().indexForRead(persistence, OBJ_REF_SERIALIZER));
  }

  private Optional<Index<IndexKey>> supplyIdIndex() {
    return refObj()
        .map(ref -> ref.stableIdToName().indexForRead(persistence, INDEX_KEY_SERIALIZER));
  }
}
