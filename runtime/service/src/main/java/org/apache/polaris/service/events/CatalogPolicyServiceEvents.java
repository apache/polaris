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

import org.apache.polaris.service.types.AttachPolicyRequest;
import org.apache.polaris.service.types.CreatePolicyRequest;
import org.apache.polaris.service.types.DetachPolicyRequest;
import org.apache.polaris.service.types.GetApplicablePoliciesResponse;
import org.apache.polaris.service.types.LoadPolicyResponse;
import org.apache.polaris.service.types.UpdatePolicyRequest;

/**
 * Event records for Catalog Policy operations. Each operation has corresponding "Before" and
 * "After" event records.
 */
public class CatalogPolicyServiceEvents {

  // Policy CRUD Events
  public record BeforeCreatePolicyEvent(
      String catalogName, String namespace, CreatePolicyRequest createPolicyRequest) {}

  public record AfterCreatePolicyEvent(
      String catalogName, String namespace, LoadPolicyResponse loadPolicyResponse) {}

  public record BeforeListPoliciesEvent(String catalogName, String namespace, String policyType) {}

  public record AfterListPoliciesEvent(String catalogName, String namespace, String policyType) {}

  public record BeforeLoadPolicyEvent(String catalogName, String namespace, String policyName) {}

  public record AfterLoadPolicyEvent(
      String catalogName, String namespace, LoadPolicyResponse loadPolicyResponse) {}

  public record BeforeUpdatePolicyEvent(
      String catalogName,
      String namespace,
      String policyName,
      UpdatePolicyRequest updatePolicyRequest) {}

  public record AfterUpdatePolicyEvent(
      String catalogName, String namespace, LoadPolicyResponse loadPolicyResponse) {}

  public record BeforeDropPolicyEvent(
      String catalogName, String namespace, String policyName, Boolean detachAll) {}

  public record AfterDropPolicyEvent(
      String catalogName, String namespace, String policyName, Boolean detachAll) {}

  // Policy Attachment Events
  public record BeforeAttachPolicyEvent(
      String catalogName,
      String namespace,
      String policyName,
      AttachPolicyRequest attachPolicyRequest) {}

  public record AfterAttachPolicyEvent(
      String catalogName,
      String namespace,
      String policyName,
      AttachPolicyRequest attachPolicyRequest) {}

  public record BeforeDetachPolicyEvent(
      String catalogName,
      String namespace,
      String policyName,
      DetachPolicyRequest detachPolicyRequest) {}

  public record AfterDetachPolicyEvent(
      String catalogName,
      String namespace,
      String policyName,
      DetachPolicyRequest detachPolicyRequest) {}

  // Policy Query Events
  public record BeforeGetApplicablePoliciesEvent(
      String catalogName, String namespace, String targetName, String policyType) {}

  public record AfterGetApplicablePoliciesEvent(
      String catalogName,
      String namespace,
      String targetName,
      String policyType,
      GetApplicablePoliciesResponse getApplicablePoliciesResponse) {}
}
