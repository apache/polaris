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
import com.fasterxml.jackson.annotation.JsonValue;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;

/** List of privileges */
public enum PolarisPrivilege {
  SERVICE_MANAGE_ACCESS(1, PolarisEntityType.ROOT),
  CATALOG_MANAGE_ACCESS(2, PolarisEntityType.CATALOG),
  CATALOG_ROLE_USAGE(
      3,
      PolarisEntityType.CATALOG_ROLE,
      PolarisEntitySubType.NULL_SUBTYPE,
      PolarisEntityType.PRINCIPAL_ROLE),
  PRINCIPAL_ROLE_USAGE(
      4,
      PolarisEntityType.PRINCIPAL_ROLE,
      PolarisEntitySubType.NULL_SUBTYPE,
      PolarisEntityType.PRINCIPAL),
  NAMESPACE_CREATE(5, PolarisEntityType.NAMESPACE),
  TABLE_CREATE(6, PolarisEntityType.NAMESPACE),
  VIEW_CREATE(7, PolarisEntityType.NAMESPACE),
  NAMESPACE_DROP(8, PolarisEntityType.NAMESPACE),
  TABLE_DROP(
      9,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  VIEW_DROP(10, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_VIEW),
  NAMESPACE_LIST(11, PolarisEntityType.NAMESPACE),
  TABLE_LIST(12, PolarisEntityType.NAMESPACE),
  VIEW_LIST(13, PolarisEntityType.NAMESPACE),
  NAMESPACE_READ_PROPERTIES(14, PolarisEntityType.NAMESPACE),
  TABLE_READ_PROPERTIES(
      15,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  VIEW_READ_PROPERTIES(16, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_VIEW),
  NAMESPACE_WRITE_PROPERTIES(17, PolarisEntityType.NAMESPACE),
  TABLE_WRITE_PROPERTIES(
      18,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  VIEW_WRITE_PROPERTIES(19, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_VIEW),
  TABLE_READ_DATA(
      20,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_WRITE_DATA(
      21,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  NAMESPACE_FULL_METADATA(22, PolarisEntityType.NAMESPACE),
  TABLE_FULL_METADATA(
      23,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  VIEW_FULL_METADATA(24, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_VIEW),
  CATALOG_CREATE(25, PolarisEntityType.ROOT),
  CATALOG_DROP(26, PolarisEntityType.CATALOG),
  CATALOG_LIST(27, PolarisEntityType.ROOT),
  CATALOG_READ_PROPERTIES(28, PolarisEntityType.CATALOG),
  CATALOG_WRITE_PROPERTIES(29, PolarisEntityType.CATALOG),
  CATALOG_FULL_METADATA(30, PolarisEntityType.CATALOG),
  CATALOG_MANAGE_METADATA(31, PolarisEntityType.CATALOG),
  CATALOG_MANAGE_CONTENT(32, PolarisEntityType.CATALOG),
  PRINCIPAL_LIST_GRANTS(33, PolarisEntityType.PRINCIPAL),
  PRINCIPAL_ROLE_LIST_GRANTS(34, PolarisEntityType.PRINCIPAL),
  CATALOG_ROLE_LIST_GRANTS(35, PolarisEntityType.PRINCIPAL),
  CATALOG_LIST_GRANTS(36, PolarisEntityType.CATALOG),
  NAMESPACE_LIST_GRANTS(37, PolarisEntityType.NAMESPACE),
  TABLE_LIST_GRANTS(
      38,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  VIEW_LIST_GRANTS(39, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_VIEW),
  CATALOG_MANAGE_GRANTS_ON_SECURABLE(40, PolarisEntityType.CATALOG),
  NAMESPACE_MANAGE_GRANTS_ON_SECURABLE(41, PolarisEntityType.NAMESPACE),
  TABLE_MANAGE_GRANTS_ON_SECURABLE(
      42,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  VIEW_MANAGE_GRANTS_ON_SECURABLE(
      43, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_VIEW),
  PRINCIPAL_CREATE(44, PolarisEntityType.ROOT),
  PRINCIPAL_DROP(45, PolarisEntityType.PRINCIPAL),
  PRINCIPAL_LIST(46, PolarisEntityType.ROOT),
  PRINCIPAL_READ_PROPERTIES(47, PolarisEntityType.PRINCIPAL),
  PRINCIPAL_WRITE_PROPERTIES(48, PolarisEntityType.PRINCIPAL),
  PRINCIPAL_FULL_METADATA(49, PolarisEntityType.PRINCIPAL),
  PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE(50, PolarisEntityType.PRINCIPAL),
  PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE(51, PolarisEntityType.PRINCIPAL),
  PRINCIPAL_ROTATE_CREDENTIALS(52, PolarisEntityType.PRINCIPAL),
  PRINCIPAL_RESET_CREDENTIALS(53, PolarisEntityType.PRINCIPAL),
  PRINCIPAL_ROLE_CREATE(54, PolarisEntityType.ROOT),
  PRINCIPAL_ROLE_DROP(55, PolarisEntityType.PRINCIPAL_ROLE),
  PRINCIPAL_ROLE_LIST(56, PolarisEntityType.ROOT),
  PRINCIPAL_ROLE_READ_PROPERTIES(57, PolarisEntityType.PRINCIPAL_ROLE),
  PRINCIPAL_ROLE_WRITE_PROPERTIES(58, PolarisEntityType.PRINCIPAL_ROLE),
  PRINCIPAL_ROLE_FULL_METADATA(59, PolarisEntityType.PRINCIPAL_ROLE),
  PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE(60, PolarisEntityType.PRINCIPAL_ROLE),
  PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE(61, PolarisEntityType.PRINCIPAL_ROLE),
  CATALOG_ROLE_CREATE(62, PolarisEntityType.CATALOG),
  CATALOG_ROLE_DROP(63, PolarisEntityType.CATALOG_ROLE),
  CATALOG_ROLE_LIST(64, PolarisEntityType.CATALOG),
  CATALOG_ROLE_READ_PROPERTIES(65, PolarisEntityType.CATALOG_ROLE),
  CATALOG_ROLE_WRITE_PROPERTIES(66, PolarisEntityType.CATALOG_ROLE),
  CATALOG_ROLE_FULL_METADATA(67, PolarisEntityType.CATALOG_ROLE),
  CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE(68, PolarisEntityType.CATALOG_ROLE),
  CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE(69, PolarisEntityType.CATALOG_ROLE),
  POLICY_CREATE(70, PolarisEntityType.NAMESPACE),
  POLICY_READ(71, PolarisEntityType.POLICY),
  POLICY_DROP(72, PolarisEntityType.POLICY),
  POLICY_WRITE(73, PolarisEntityType.POLICY),
  POLICY_LIST(74, PolarisEntityType.NAMESPACE),
  POLICY_FULL_METADATA(75, PolarisEntityType.POLICY),
  POLICY_ATTACH(76, PolarisEntityType.POLICY),
  POLICY_DETACH(77, PolarisEntityType.POLICY),
  CATALOG_ATTACH_POLICY(78, PolarisEntityType.CATALOG),
  NAMESPACE_ATTACH_POLICY(79, PolarisEntityType.NAMESPACE),
  TABLE_ATTACH_POLICY(80, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE),
  CATALOG_DETACH_POLICY(81, PolarisEntityType.CATALOG),
  NAMESPACE_DETACH_POLICY(82, PolarisEntityType.NAMESPACE),
  TABLE_DETACH_POLICY(83, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE),
  POLICY_MANAGE_GRANTS_ON_SECURABLE(
      84,
      PolarisEntityType.POLICY,
      PolarisEntitySubType.NULL_SUBTYPE,
      PolarisEntityType.CATALOG_ROLE),
  TABLE_ASSIGN_UUID(
      85,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_UPGRADE_FORMAT_VERSION(
      86,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_ADD_SCHEMA(
      87,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_SET_CURRENT_SCHEMA(
      88,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_ADD_PARTITION_SPEC(
      89,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_ADD_SORT_ORDER(
      90,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_SET_DEFAULT_SORT_ORDER(
      91,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_ADD_SNAPSHOT(
      92,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_SET_SNAPSHOT_REF(
      93,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_REMOVE_SNAPSHOTS(
      94,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_REMOVE_SNAPSHOT_REF(
      95,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_SET_LOCATION(
      96,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_SET_PROPERTIES(
      97,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_REMOVE_PROPERTIES(
      98,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_SET_STATISTICS(
      99,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_REMOVE_STATISTICS(
      100,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_REMOVE_PARTITION_SPECS(
      101,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_MANAGE_STRUCTURE(
      102,
      PolarisEntityType.TABLE_LIKE,
      List.of(PolarisEntitySubType.ICEBERG_TABLE, PolarisEntitySubType.GENERIC_TABLE),
      PolarisEntityType.CATALOG_ROLE),
  TABLE_REMOTE_SIGN(103, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.ICEBERG_TABLE),
  ;

  /**
   * Full constructor
   *
   * @param code internal code associated to this privilege
   * @param securableType securable type
   * @param securableSubTypes securable subtypes, mostly NULL_SUBTYPE
   * @param granteeType grantee type, generally a ROLE
   */
  PolarisPrivilege(
      int code,
      @Nonnull PolarisEntityType securableType,
      @Nonnull List<PolarisEntitySubType> securableSubTypes,
      @Nonnull PolarisEntityType granteeType) {
    this.code = code;
    this.securableType = securableType;
    this.securableSubTypes = securableSubTypes;
    this.granteeType = granteeType;
  }

  /**
   * Shorthand for a single securable subtype
   *
   * @param code internal code associated to this privilege
   * @param securableType securable type
   * @param securableSubType securable subtype, mostly NULL_SUBTYPE
   * @param granteeType grantee type, generally a ROLE
   */
  PolarisPrivilege(
      int code,
      @Nonnull PolarisEntityType securableType,
      @Nonnull PolarisEntitySubType securableSubType,
      @Nonnull PolarisEntityType granteeType) {
    this(code, securableType, List.of(securableSubType), granteeType);
  }

  /**
   * Simple constructor, when the grantee is a role and the securable subtype is NULL_SUBTYPE
   *
   * @param code internal code associated to this privilege
   * @param securableType securable type
   */
  PolarisPrivilege(int code, @Nonnull PolarisEntityType securableType) {
    this(
        code,
        securableType,
        List.of(PolarisEntitySubType.NULL_SUBTYPE),
        PolarisEntityType.CATALOG_ROLE);
  }

  /**
   * Constructor when the grantee is a ROLE
   *
   * @param code internal code associated to this privilege
   * @param securableType securable type
   * @param securableSubType securable subtype, mostly NULL_SUBTYPE
   */
  PolarisPrivilege(
      int code,
      @Nonnull PolarisEntityType securableType,
      @Nonnull PolarisEntitySubType securableSubType) {
    this(code, securableType, List.of(securableSubType), PolarisEntityType.CATALOG_ROLE);
  }

  // internal code used to represent this privilege
  private final int code;

  // the type of the securable for this privilege
  private final PolarisEntityType securableType;

  // the subtype of the securable for this privilege
  private final List<PolarisEntitySubType> securableSubTypes;

  // the type of the securable for this privilege
  private final PolarisEntityType granteeType;

  // to efficiently map a code to its corresponding entity type, use a reverse array which
  // is initialized below
  private static final PolarisPrivilege[] REVERSE_MAPPING_ARRAY;

  static {
    // find max array size
    int maxId = 0;
    for (PolarisPrivilege privilegeDef : PolarisPrivilege.values()) {
      if (maxId < privilegeDef.code) {
        maxId = privilegeDef.code;
      }
    }

    // allocate mapping array
    REVERSE_MAPPING_ARRAY = new PolarisPrivilege[maxId + 1];

    // populate mapping array
    for (PolarisPrivilege privilegeDef : PolarisPrivilege.values()) {
      REVERSE_MAPPING_ARRAY[privilegeDef.code] = privilegeDef;
    }
  }

  /**
   * @return the code associated to the specified privilege
   */
  @JsonValue
  public int getCode() {
    return code;
  }

  /**
   * Given the code associated to a privilege, return the privilege associated to it. Return null if
   * not found
   *
   * @param code code associated to the entity type
   * @return entity type corresponding to that code or null if mapping not found
   */
  @JsonCreator
  public static @Nullable PolarisPrivilege fromCode(int code) {
    // ensure it is within bounds
    if (code < 0 || code >= REVERSE_MAPPING_ARRAY.length) {
      return null;
    }

    // get value
    return REVERSE_MAPPING_ARRAY[code];
  }
}
