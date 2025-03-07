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
 * Users can either extend this interface and implement handlers for all events or, for ease, extend
 * DefaultPolarisEventListener and only have to handle a subset of events. Event details are
 * documented under the event objects themselves.
 */
public interface PolarisEventListener {
  /** {@link BeforeRequestRateLimitedEvent} */
  void onBeforeRequestRateLimited(BeforeRequestRateLimitedEvent event);

  /** {@link BeforeTableCommitedEvent} */
  void onBeforeTableCommited(BeforeTableCommitedEvent event);

  /** {@link AfterTableCommitedEvent} */
  void onAfterTableCommited(AfterTableCommitedEvent event);

  /** {@link BeforeViewCommitedEvent} */
  void onBeforeViewCommited(BeforeViewCommitedEvent event);

  /** {@link AfterViewCommitedEvent} */
  void onAfterViewCommited(AfterViewCommitedEvent event);

  /** {@link BeforeTableRefreshedEvent} */
  void onBeforeTableRefreshed(BeforeTableRefreshedEvent event);

  /** {@link AfterTableRefreshedEvent} */
  void onAfterTableRefreshed(AfterTableRefreshedEvent event);

  /** {@link BeforeViewRefreshedEvent} */
  void onBeforeViewRefreshed(BeforeViewRefreshedEvent event);

  /** {@link AfterViewRefreshedEvent} */
  void onAfterViewRefreshed(AfterViewRefreshedEvent event);

  /** {@link BeforeTaskAttemptedEvent} */
  void onBeforeTaskAttempted(BeforeTaskAttemptedEvent event);

  /** {@link AfterTaskAttemptedEvent} */
  void onAfterTaskAttempted(AfterTaskAttemptedEvent event);
}
