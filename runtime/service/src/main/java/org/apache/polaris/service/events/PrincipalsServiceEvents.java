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

package org.apache.polaris.service.events;

import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;

public class PrincipalsServiceEvents {
  public record AfterCreatePrincipalEvent(Principal principal) implements PolarisEvent {}

  public record BeforeCreatePrincipalEvent(String principalName) implements PolarisEvent {}

  public record AfterDeletePrincipalEvent(String principalName) implements PolarisEvent {}

  public record BeforeDeletePrincipalEvent(String principalName) implements PolarisEvent {}

  public record AfterGetPrincipalEvent(Principal principal) implements PolarisEvent {}

  public record BeforeGetPrincipalEvent(String principalName) implements PolarisEvent {}

  public record AfterUpdatePrincipalEvent(Principal principal) implements PolarisEvent {}

  public record BeforeUpdatePrincipalEvent(
      String principalName, UpdatePrincipalRequest updatePrincipalRequest)
      implements PolarisEvent {}

  public record AfterRotateCredentialsEvent(Principal rotatedPrincipal) implements PolarisEvent {}

  public record BeforeRotateCredentialsEvent(String principalName) implements PolarisEvent {}

  public record AfterListPrincipalsEvent() implements PolarisEvent {}

  public record BeforeListPrincipalsEvent() implements PolarisEvent {}

  public record AfterAssignPrincipalRoleEvent(String principalName, PrincipalRole principalRole)
      implements PolarisEvent {}

  public record BeforeAssignPrincipalRoleEvent(String principalName, PrincipalRole principalRole)
      implements PolarisEvent {}

  public record AfterRevokePrincipalRoleEvent(String principalName, String principalRoleName)
      implements PolarisEvent {}

  public record BeforeRevokePrincipalRoleEvent(String principalName, String principalRoleName)
      implements PolarisEvent {}

  public record AfterListAssignedPrincipalRolesEvent(String principalName)
      implements PolarisEvent {}

  public record BeforeListAssignedPrincipalRolesEvent(String principalName)
      implements PolarisEvent {}

  public record BeforeResetCredentialsEvent(String principalName) implements PolarisEvent {}

  public record AfterResetCredentialsEvent(Principal principal) implements PolarisEvent {}
}
