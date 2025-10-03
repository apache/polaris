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

import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.persistence.nosql.coretypes.acl.AclObj;

/**
 * Holds IDs needed during ACL/grants loading, before those are exploded into many {@link
 * PolarisGrantRecord}s.
 */
record SecurableAndGrantee(
    long securableCatalogId,
    long securableId,
    int securableTypeCode,
    long granteeCatalogId,
    long granteeId,
    int granteeTypeCode) {

  static SecurableAndGrantee forTriplet(long catalogId, AclObj aclObj, GrantTriplet triplet) {
    var reverseOrKey = triplet.reverseOrKey();
    var securableCatalogId = reverseOrKey ? triplet.catalogId() : catalogId;
    var securableId = reverseOrKey ? triplet.id() : aclObj.securableId();
    var securableTypeCode =
        reverseOrKey ? triplet.typeCode() : aclObj.securableTypeCode().orElseThrow();
    var granteeCatalogId = reverseOrKey ? catalogId : triplet.catalogId();
    var granteeId = reverseOrKey ? aclObj.securableId() : triplet.id();
    var granteeTypeCode =
        reverseOrKey ? aclObj.securableTypeCode().orElseThrow() : triplet.typeCode();
    return new SecurableAndGrantee(
        securableCatalogId,
        securableId,
        securableTypeCode,
        granteeCatalogId,
        granteeId,
        granteeTypeCode);
  }

  PolarisGrantRecord grantRecordForPrivilege(int privilegeCode) {
    return new PolarisGrantRecord(
        securableCatalogId, securableId, granteeCatalogId, granteeId, privilegeCode);
  }
}
