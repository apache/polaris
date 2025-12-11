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
package org.apache.polaris.persistence.nosql.metastore.privs;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.coretypes.ObjBase;
import org.apache.polaris.persistence.nosql.coretypes.acl.GrantsObj;

/**
 * Represents the triplet of catalog-ID, entity-ID and type-code plus a reverse-or-key marker.
 *
 * <p>This is intended to construct {@link IndexKey}s for {@link GrantsObj#acls}, which contain the
 * "reversed" and "directed" mappings.
 *
 * <ul>
 *   <li>"Reversed" ({@code reverseOrKey == true}, role encoding {@code 'r'}) means <em>from</em> a
 *       grantee or securable.
 *   <li>"Directed" ({@code reverseOrKey == false}, role encoding {@code 'd'}) means <em>to</em> a
 *       grantee (no use case for <em>to</em> securable).
 * </ul>
 *
 * <p>String representations of this type are used as ACL names and "role" names.
 *
 * @param reverseOrKey
 * @param catalogId catalog id
 * @param id entity id (aka {@link ObjBase#stableId()}
 * @param typeCode {@link PolarisEntityType#getCode() entity type code}
 */
public record GrantTriplet(boolean reverseOrKey, long catalogId, long id, int typeCode) {

  /**
   * Constructs a new {@link GrantTriplet} instance for the given entity, with {@link
   * #reverseOrKey()} set to {@code true}.
   */
  public static GrantTriplet forEntity(PolarisEntityCore entity) {
    return new GrantTriplet(true, entity.getCatalogId(), entity.getId(), entity.getTypeCode());
  }

  /** Convert to a "directed" grant-triplet, having {@link #reverseOrKey()} set to {@code false}. */
  public GrantTriplet asDirected() {
    return new GrantTriplet(false, catalogId, id, typeCode);
  }

  /**
   * Constructs the role name, the encoded string representation, for this triplet in the pattern
   * {@code [r|d] "/" catalogId "/" id "/" typeCode}
   */
  public String toRoleName() {
    return (reverseOrKey ? "r/" : "d/") + catalogId + "/" + id + "/" + typeCode;
  }

  /**
   * Parses a {@link GrantTriplet#toRoleName()}, expecting exactly the pattern {@code [r|d] "/"
   * catalogId "/" id "/" typeCode}.
   */
  public static GrantTriplet fromRoleName(String roleName) {
    var c0 = roleName.charAt(0);
    checkArgument(roleName.charAt(1) == '/' && (c0 == 'r' || c0 == 'd'));

    var idx2 = roleName.indexOf('/', 2);
    var idx3 = roleName.indexOf('/', idx2 + 1);

    var catalogId = Long.parseLong(roleName.substring(2, idx2));
    var id = Long.parseLong(roleName.substring(idx2 + 1, idx3));
    var typeCode = Integer.parseInt(roleName.substring(idx3 + 1));

    var reversed = c0 == 'r';

    return new GrantTriplet(reversed, catalogId, id, typeCode);
  }
}
