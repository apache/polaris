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
package org.apache.polaris.persistence.nosql.impl.indexes;

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;

import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

final class LazyIndexElementJCStressSupport {
  static final IndexKey ITERATOR_KEY_1 = key("key-1");
  static final IndexKey ITERATOR_KEY_2 = key("key-2");
  static final IndexKey ITERATOR_KEY_3 = key("key-3");

  private LazyIndexElementJCStressSupport() {}

  static ObjRef value() {
    return objRef("test", 42L, 1);
  }

  static InternalIndexElement<ObjRef> singleElement(IndexKey key, ObjRef value) {
    return singleElementIndex(key, value).elementIterator().next();
  }

  static IndexSpi<ObjRef> singleElementIndex(IndexKey key, ObjRef value) {
    var index = newStoreIndex(OBJ_REF_SERIALIZER);
    index.add(indexElement(key, value));
    return deserializeStoreIndex(index.serialize(), OBJ_REF_SERIALIZER);
  }

  static IndexSpi<ObjRef> iteratorIndex() {
    var index = newStoreIndex(OBJ_REF_SERIALIZER);
    index.add(indexElement(ITERATOR_KEY_1, value()));
    index.add(indexElement(ITERATOR_KEY_2, null));
    index.add(indexElement(ITERATOR_KEY_3, value()));
    return deserializeStoreIndex(index.serialize(), OBJ_REF_SERIALIZER);
  }

  static int count(Index<ObjRef> index) {
    var count = 0;
    for (Index.Element<ObjRef> ignored : index) {
      count++;
    }
    return count;
  }
}
