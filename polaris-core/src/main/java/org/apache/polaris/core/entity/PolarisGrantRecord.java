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
package org.apache.polaris.core.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PolarisGrantRecord {

  // id of the catalog where the securable entity resides, NULL_ID if this entity is a top-level
  // account entity
  private long securableCatalogId;

  // id of the securable
  private long securableId;

  // id of the catalog where the grantee entity resides, NULL_ID if this entity is a top-level
  // account entity
  private long granteeCatalogId;

  // id of the grantee
  private long granteeId;

  // id associated to the privilege
  private int privilegeCode;

  public PolarisGrantRecord() {}

  public long getSecurableCatalogId() {
    return securableCatalogId;
  }

  public void setSecurableCatalogId(long securableCatalogId) {
    this.securableCatalogId = securableCatalogId;
  }

  public long getSecurableId() {
    return securableId;
  }

  public void setSecurableId(long securableId) {
    this.securableId = securableId;
  }

  public long getGranteeCatalogId() {
    return granteeCatalogId;
  }

  public void setGranteeCatalogId(long granteeCatalogId) {
    this.granteeCatalogId = granteeCatalogId;
  }

  public long getGranteeId() {
    return granteeId;
  }

  public void setGranteeId(long granteeId) {
    this.granteeId = granteeId;
  }

  public int getPrivilegeCode() {
    return privilegeCode;
  }

  public void setPrivilegeCode(int privilegeCode) {
    this.privilegeCode = privilegeCode;
  }

  /**
   * Constructor
   *
   * @param securableCatalogId catalog id for the securable. Can be NULL_ID if securable is
   *     top-level account entity
   * @param securableId id of the securable
   * @param granteeCatalogId catalog id for the grantee, Can be NULL_ID if grantee is top-level
   *     account entity
   * @param granteeId id of the grantee
   * @param privilegeCode privilege being granted to the grantee on the securable
   */
  @JsonCreator
  public PolarisGrantRecord(
      @JsonProperty("securableCatalogId") long securableCatalogId,
      @JsonProperty("securableId") long securableId,
      @JsonProperty("granteeCatalogId") long granteeCatalogId,
      @JsonProperty("granteeId") long granteeId,
      @JsonProperty("privilegeCode") int privilegeCode) {
    this.securableCatalogId = securableCatalogId;
    this.securableId = securableId;
    this.granteeCatalogId = granteeCatalogId;
    this.granteeId = granteeId;
    this.privilegeCode = privilegeCode;
  }

  /**
   * Copy constructor
   *
   * @param grantRec grant rec to copy
   */
  public PolarisGrantRecord(PolarisGrantRecord grantRec) {
    this.securableCatalogId = grantRec.getSecurableCatalogId();
    this.securableId = grantRec.getSecurableId();
    this.granteeCatalogId = grantRec.getGranteeCatalogId();
    this.granteeId = grantRec.getGranteeId();
    this.privilegeCode = grantRec.getPrivilegeCode();
  }

  @Override
  public String toString() {
    return "PolarisGrantRec{"
        + "securableCatalogId="
        + securableCatalogId
        + ", securableId="
        + securableId
        + ", granteeCatalogId="
        + granteeCatalogId
        + ", granteeId="
        + granteeId
        + ", privilegeCode="
        + privilegeCode
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PolarisGrantRecord)) return false;
    PolarisGrantRecord that = (PolarisGrantRecord) o;
    return securableCatalogId == that.securableCatalogId
        && securableId == that.securableId
        && granteeCatalogId == that.granteeCatalogId
        && granteeId == that.granteeId
        && privilegeCode == that.privilegeCode;
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(
        securableCatalogId, securableId, granteeCatalogId, granteeId, privilegeCode);
  }
}
