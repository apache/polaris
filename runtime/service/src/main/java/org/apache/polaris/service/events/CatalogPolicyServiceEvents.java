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

import java.util.UUID;
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
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      CreatePolicyRequest createPolicyRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CREATE_POLICY;
    }
  }

  public record AfterCreatePolicyEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      LoadPolicyResponse loadPolicyResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CREATE_POLICY;
    }
  }

  public record BeforeListPoliciesEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String policyType)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_POLICIES;
    }
  }

  public record AfterListPoliciesEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String policyType)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_POLICIES;
    }
  }

  public record BeforeLoadPolicyEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String policyName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LOAD_POLICY;
    }
  }

  public record AfterLoadPolicyEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      LoadPolicyResponse loadPolicyResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LOAD_POLICY;
    }
  }

  public record BeforeUpdatePolicyEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String policyName,
      UpdatePolicyRequest updatePolicyRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_UPDATE_POLICY;
    }
  }

  public record AfterUpdatePolicyEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      LoadPolicyResponse loadPolicyResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_UPDATE_POLICY;
    }
  }

  public record BeforeDropPolicyEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String policyName,
      Boolean detachAll)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DROP_POLICY;
    }
  }

  public record AfterDropPolicyEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String policyName,
      Boolean detachAll)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DROP_POLICY;
    }
  }

  // Policy Attachment Events
  public record BeforeAttachPolicyEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String policyName,
      AttachPolicyRequest attachPolicyRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_ATTACH_POLICY;
    }
  }

  public record AfterAttachPolicyEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String policyName,
      AttachPolicyRequest attachPolicyRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_ATTACH_POLICY;
    }
  }

  public record BeforeDetachPolicyEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String policyName,
      DetachPolicyRequest detachPolicyRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DETACH_POLICY;
    }
  }

  public record AfterDetachPolicyEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String policyName,
      DetachPolicyRequest detachPolicyRequest)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DETACH_POLICY;
    }
  }

  // Policy Query Events
  public record BeforeGetApplicablePoliciesEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String targetName,
      String policyType)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_GET_APPLICABLE_POLICIES;
    }
  }

  public record AfterGetApplicablePoliciesEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String targetName,
      String policyType,
      GetApplicablePoliciesResponse getApplicablePoliciesResponse)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_GET_APPLICABLE_POLICIES;
    }
  }
}
