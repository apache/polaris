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
package org.apache.polaris.service.events.listeners;

import org.apache.polaris.service.events.AfterCatalogCreatedEvent;
import org.apache.polaris.service.events.AfterTableCommitedEvent;
import org.apache.polaris.service.events.AfterTableCreatedEvent;
import org.apache.polaris.service.events.AfterTableRefreshedEvent;
import org.apache.polaris.service.events.AfterTaskAttemptedEvent;
import org.apache.polaris.service.events.AfterViewCommitedEvent;
import org.apache.polaris.service.events.AfterViewRefreshedEvent;
import org.apache.polaris.service.events.BeforeRequestRateLimitedEvent;
import org.apache.polaris.service.events.BeforeTableCommitedEvent;
import org.apache.polaris.service.events.BeforeTableRefreshedEvent;
import org.apache.polaris.service.events.BeforeTaskAttemptedEvent;
import org.apache.polaris.service.events.BeforeViewCommitedEvent;
import org.apache.polaris.service.events.BeforeViewRefreshedEvent;
import org.apache.polaris.service.events.CatalogGenericTableServiceEvents;
import org.apache.polaris.service.events.CatalogPolicyServiceEvents;

/**
 * Represents an event listener that can respond to notable moments during Polaris's execution.
 * Event details are documented under the event objects themselves.
 */
public abstract class PolarisEventListener {

  /** {@link BeforeRequestRateLimitedEvent} */
  public void onBeforeRequestRateLimited(BeforeRequestRateLimitedEvent event) {}

  /** {@link BeforeTableCommitedEvent} */
  public void onBeforeTableCommited(BeforeTableCommitedEvent event) {}

  /** {@link AfterTableCommitedEvent} */
  public void onAfterTableCommited(AfterTableCommitedEvent event) {}

  /** {@link BeforeViewCommitedEvent} */
  public void onBeforeViewCommited(BeforeViewCommitedEvent event) {}

  /** {@link AfterViewCommitedEvent} */
  public void onAfterViewCommited(AfterViewCommitedEvent event) {}

  /** {@link BeforeTableRefreshedEvent} */
  public void onBeforeTableRefreshed(BeforeTableRefreshedEvent event) {}

  /** {@link AfterTableRefreshedEvent} */
  public void onAfterTableRefreshed(AfterTableRefreshedEvent event) {}

  /** {@link BeforeViewRefreshedEvent} */
  public void onBeforeViewRefreshed(BeforeViewRefreshedEvent event) {}

  /** {@link AfterViewRefreshedEvent} */
  public void onAfterViewRefreshed(AfterViewRefreshedEvent event) {}

  /** {@link BeforeTaskAttemptedEvent} */
  public void onBeforeTaskAttempted(BeforeTaskAttemptedEvent event) {}

  /** {@link AfterTaskAttemptedEvent} */
  public void onAfterTaskAttempted(AfterTaskAttemptedEvent event) {}

  /** {@link AfterTableCreatedEvent} */
  public void onAfterTableCreated(AfterTableCreatedEvent event) {}

  /** {@link AfterCatalogCreatedEvent} */
  public void onAfterCatalogCreated(AfterCatalogCreatedEvent event) {}

  // Catalog Policy Service Events
  /** {@link CatalogPolicyServiceEvents.BeforeCreatePolicyEvent} */
  public void onBeforeCreatePolicy(CatalogPolicyServiceEvents.BeforeCreatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterCreatePolicyEvent} */
  public void onAfterCreatePolicy(CatalogPolicyServiceEvents.AfterCreatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeListPoliciesEvent} */
  public void onBeforeListPolicies(CatalogPolicyServiceEvents.BeforeListPoliciesEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterListPoliciesEvent} */
  public void onAfterListPolicies(CatalogPolicyServiceEvents.AfterListPoliciesEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeLoadPolicyEvent} */
  public void onBeforeLoadPolicy(CatalogPolicyServiceEvents.BeforeLoadPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterLoadPolicyEvent} */
  public void onAfterLoadPolicy(CatalogPolicyServiceEvents.AfterLoadPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeUpdatePolicyEvent} */
  public void onBeforeUpdatePolicy(CatalogPolicyServiceEvents.BeforeUpdatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterUpdatePolicyEvent} */
  public void onAfterUpdatePolicy(CatalogPolicyServiceEvents.AfterUpdatePolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeDropPolicyEvent} */
  public void onBeforeDropPolicy(CatalogPolicyServiceEvents.BeforeDropPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterDropPolicyEvent} */
  public void onAfterDropPolicy(CatalogPolicyServiceEvents.AfterDropPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeAttachPolicyEvent} */
  public void onBeforeAttachPolicy(CatalogPolicyServiceEvents.BeforeAttachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterAttachPolicyEvent} */
  public void onAfterAttachPolicy(CatalogPolicyServiceEvents.AfterAttachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeDetachPolicyEvent} */
  public void onBeforeDetachPolicy(CatalogPolicyServiceEvents.BeforeDetachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterDetachPolicyEvent} */
  public void onAfterDetachPolicy(CatalogPolicyServiceEvents.AfterDetachPolicyEvent event) {}

  /** {@link CatalogPolicyServiceEvents.BeforeGetApplicablePoliciesEvent} */
  public void onBeforeGetApplicablePolicies(
      CatalogPolicyServiceEvents.BeforeGetApplicablePoliciesEvent event) {}

  /** {@link CatalogPolicyServiceEvents.AfterGetApplicablePoliciesEvent} */
  public void onAfterGetApplicablePolicies(
      CatalogPolicyServiceEvents.AfterGetApplicablePoliciesEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeCreateGenericTableEvent} */
  public void onBeforeCreateGenericTable(
      CatalogGenericTableServiceEvents.BeforeCreateGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterCreateGenericTableEvent} */
  public void onAfterCreateGenericTable(
      CatalogGenericTableServiceEvents.AfterCreateGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeDropGenericTableEvent} */
  public void onBeforeDropGenericTable(
      CatalogGenericTableServiceEvents.BeforeDropGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterDropGenericTableEvent} */
  public void onAfterDropGenericTable(
      CatalogGenericTableServiceEvents.AfterDropGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeListGenericTablesEvent} */
  public void onBeforeListGenericTables(
      CatalogGenericTableServiceEvents.BeforeListGenericTablesEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterListGenericTablesEvent} */
  public void onAfterListGenericTables(
      CatalogGenericTableServiceEvents.AfterListGenericTablesEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.BeforeLoadGenericTableEvent} */
  public void onBeforeLoadGenericTable(
      CatalogGenericTableServiceEvents.BeforeLoadGenericTableEvent event) {}

  /** {@link CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent} */
  public void onAfterLoadGenericTable(
      CatalogGenericTableServiceEvents.AfterLoadGenericTableEvent event) {}
}
