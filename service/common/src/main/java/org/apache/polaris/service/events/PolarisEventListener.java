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
}
