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

import static org.apache.polaris.persistence.nosql.metastore.ContentIdentifier.indexKeyToIdentifier;

import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.metastore.ContentIdentifier;

public final class IndexUtils {
  private IndexUtils() {}

  public static boolean hasChildren(
      long catalogId, Index<ObjRef> nameIndex, Index<IndexKey> stableIdIndex, long parentId) {
    if (nameIndex != null) {
      if (parentId == 0L || parentId == catalogId) {
        return nameIndex.iterator().hasNext();
      } else {
        var parentNameKey =
            stableIdIndex != null ? stableIdIndex.get(IndexKey.key(parentId)) : null;
        if (parentNameKey != null) {
          var iter = nameIndex.iterator(parentNameKey, null, false);
          // skip the parent itself
          iter.next();
          if (iter.hasNext()) {
            var e = iter.next();
            var nextKey = e.getKey();
            var parentIdent = indexKeyToIdentifier(parentNameKey);
            var nextIdent = indexKeyToIdentifier(nextKey);
            return nextIdent.parent().equals(parentIdent);
          }
        }
      }
    }
    return false;
  }

  public static boolean hasChildren(Index<ObjRef> nameIndex, ContentIdentifier ident) {
    if (ident.isEmpty()) {
      return nameIndex.iterator().hasNext();
    }
    var key = ident.toIndexKey();

    var iter = nameIndex.iterator(key, null, false);
    // skip the parent itself
    iter.next();
    if (iter.hasNext()) {
      var e = iter.next();
      var nextKey = e.getKey();
      var nextIdent = indexKeyToIdentifier(nextKey);
      return nextIdent.parent().equals(ident);
    }
    return false;
  }
}
