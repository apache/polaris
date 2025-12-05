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

import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PolarisPrivilegeTest {

  static Stream<Arguments> polarisPrivileges() {
    return Stream.of(
        Arguments.of(-1, null),
        Arguments.of(1, PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        Arguments.of(2, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        Arguments.of(3, PolarisPrivilege.CATALOG_ROLE_USAGE),
        Arguments.of(4, PolarisPrivilege.PRINCIPAL_ROLE_USAGE),
        Arguments.of(5, PolarisPrivilege.NAMESPACE_CREATE),
        Arguments.of(6, PolarisPrivilege.TABLE_CREATE),
        Arguments.of(7, PolarisPrivilege.VIEW_CREATE),
        Arguments.of(8, PolarisPrivilege.NAMESPACE_DROP),
        Arguments.of(9, PolarisPrivilege.TABLE_DROP),
        Arguments.of(10, PolarisPrivilege.VIEW_DROP),
        Arguments.of(11, PolarisPrivilege.NAMESPACE_LIST),
        Arguments.of(12, PolarisPrivilege.TABLE_LIST),
        Arguments.of(13, PolarisPrivilege.VIEW_LIST),
        Arguments.of(14, PolarisPrivilege.NAMESPACE_READ_PROPERTIES),
        Arguments.of(15, PolarisPrivilege.TABLE_READ_PROPERTIES),
        Arguments.of(16, PolarisPrivilege.VIEW_READ_PROPERTIES),
        Arguments.of(17, PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES),
        Arguments.of(18, PolarisPrivilege.TABLE_WRITE_PROPERTIES),
        Arguments.of(19, PolarisPrivilege.VIEW_WRITE_PROPERTIES),
        Arguments.of(20, PolarisPrivilege.TABLE_READ_DATA),
        Arguments.of(21, PolarisPrivilege.TABLE_WRITE_DATA),
        Arguments.of(22, PolarisPrivilege.NAMESPACE_FULL_METADATA),
        Arguments.of(23, PolarisPrivilege.TABLE_FULL_METADATA),
        Arguments.of(24, PolarisPrivilege.VIEW_FULL_METADATA),
        Arguments.of(25, PolarisPrivilege.CATALOG_CREATE),
        Arguments.of(26, PolarisPrivilege.CATALOG_DROP),
        Arguments.of(27, PolarisPrivilege.CATALOG_LIST),
        Arguments.of(28, PolarisPrivilege.CATALOG_READ_PROPERTIES),
        Arguments.of(29, PolarisPrivilege.CATALOG_WRITE_PROPERTIES),
        Arguments.of(30, PolarisPrivilege.CATALOG_FULL_METADATA),
        Arguments.of(31, PolarisPrivilege.CATALOG_MANAGE_METADATA),
        Arguments.of(32, PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        Arguments.of(33, PolarisPrivilege.PRINCIPAL_LIST_GRANTS),
        Arguments.of(34, PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS),
        Arguments.of(35, PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS),
        Arguments.of(36, PolarisPrivilege.CATALOG_LIST_GRANTS),
        Arguments.of(37, PolarisPrivilege.NAMESPACE_LIST_GRANTS),
        Arguments.of(38, PolarisPrivilege.TABLE_LIST_GRANTS),
        Arguments.of(39, PolarisPrivilege.VIEW_LIST_GRANTS),
        Arguments.of(40, PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE),
        Arguments.of(41, PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE),
        Arguments.of(42, PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE),
        Arguments.of(43, PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE),
        Arguments.of(44, PolarisPrivilege.PRINCIPAL_CREATE),
        Arguments.of(45, PolarisPrivilege.PRINCIPAL_DROP),
        Arguments.of(46, PolarisPrivilege.PRINCIPAL_LIST),
        Arguments.of(47, PolarisPrivilege.PRINCIPAL_READ_PROPERTIES),
        Arguments.of(48, PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES),
        Arguments.of(49, PolarisPrivilege.PRINCIPAL_FULL_METADATA),
        Arguments.of(50, PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE),
        Arguments.of(51, PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE),
        Arguments.of(52, PolarisPrivilege.PRINCIPAL_ROTATE_CREDENTIALS),
        Arguments.of(53, PolarisPrivilege.PRINCIPAL_RESET_CREDENTIALS),
        Arguments.of(54, PolarisPrivilege.PRINCIPAL_ROLE_CREATE),
        Arguments.of(55, PolarisPrivilege.PRINCIPAL_ROLE_DROP),
        Arguments.of(56, PolarisPrivilege.PRINCIPAL_ROLE_LIST),
        Arguments.of(57, PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES),
        Arguments.of(58, PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES),
        Arguments.of(59, PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA),
        Arguments.of(60, PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE),
        Arguments.of(61, PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
        Arguments.of(62, PolarisPrivilege.CATALOG_ROLE_CREATE),
        Arguments.of(63, PolarisPrivilege.CATALOG_ROLE_DROP),
        Arguments.of(64, PolarisPrivilege.CATALOG_ROLE_LIST),
        Arguments.of(65, PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES),
        Arguments.of(66, PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES),
        Arguments.of(67, PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        Arguments.of(68, PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE),
        Arguments.of(69, PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
        Arguments.of(70, PolarisPrivilege.POLICY_CREATE),
        Arguments.of(71, PolarisPrivilege.POLICY_READ),
        Arguments.of(72, PolarisPrivilege.POLICY_DROP),
        Arguments.of(73, PolarisPrivilege.POLICY_WRITE),
        Arguments.of(74, PolarisPrivilege.POLICY_LIST),
        Arguments.of(75, PolarisPrivilege.POLICY_FULL_METADATA),
        Arguments.of(76, PolarisPrivilege.POLICY_ATTACH),
        Arguments.of(77, PolarisPrivilege.POLICY_DETACH),
        Arguments.of(78, PolarisPrivilege.CATALOG_ATTACH_POLICY),
        Arguments.of(79, PolarisPrivilege.NAMESPACE_ATTACH_POLICY),
        Arguments.of(80, PolarisPrivilege.TABLE_ATTACH_POLICY),
        Arguments.of(81, PolarisPrivilege.CATALOG_DETACH_POLICY),
        Arguments.of(82, PolarisPrivilege.NAMESPACE_DETACH_POLICY),
        Arguments.of(83, PolarisPrivilege.TABLE_DETACH_POLICY),
        Arguments.of(84, PolarisPrivilege.POLICY_MANAGE_GRANTS_ON_SECURABLE),
        Arguments.of(85, PolarisPrivilege.TABLE_ASSIGN_UUID),
        Arguments.of(86, PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION),
        Arguments.of(87, PolarisPrivilege.TABLE_ADD_SCHEMA),
        Arguments.of(88, PolarisPrivilege.TABLE_SET_CURRENT_SCHEMA),
        Arguments.of(89, PolarisPrivilege.TABLE_ADD_PARTITION_SPEC),
        Arguments.of(90, PolarisPrivilege.TABLE_ADD_SORT_ORDER),
        Arguments.of(91, PolarisPrivilege.TABLE_SET_DEFAULT_SORT_ORDER),
        Arguments.of(92, PolarisPrivilege.TABLE_ADD_SNAPSHOT),
        Arguments.of(93, PolarisPrivilege.TABLE_SET_SNAPSHOT_REF),
        Arguments.of(94, PolarisPrivilege.TABLE_REMOVE_SNAPSHOTS),
        Arguments.of(95, PolarisPrivilege.TABLE_REMOVE_SNAPSHOT_REF),
        Arguments.of(96, PolarisPrivilege.TABLE_SET_LOCATION),
        Arguments.of(97, PolarisPrivilege.TABLE_SET_PROPERTIES),
        Arguments.of(98, PolarisPrivilege.TABLE_REMOVE_PROPERTIES),
        Arguments.of(99, PolarisPrivilege.TABLE_SET_STATISTICS),
        Arguments.of(100, PolarisPrivilege.TABLE_REMOVE_STATISTICS),
        Arguments.of(101, PolarisPrivilege.TABLE_REMOVE_PARTITION_SPECS),
        Arguments.of(102, PolarisPrivilege.TABLE_MANAGE_STRUCTURE),
        Arguments.of(103, PolarisPrivilege.TABLE_REMOTE_SIGN),
        Arguments.of(104, null));
  }

  @ParameterizedTest
  @MethodSource("polarisPrivileges")
  public void testFromCode(int code, PolarisPrivilege expected) {
    Assertions.assertThat(PolarisPrivilege.fromCode(code)).isEqualTo(expected);
  }
}
