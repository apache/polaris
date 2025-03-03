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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Event listener that does nothing. Instead of extending the PolarisEventListener interface and
 * having to implement handlers for all events, you can subclass DefaultPolarisEventListener and
 * pick which events to override.
 */
@ApplicationScoped
@Identifier("default")
public class DefaultPolarisEventListener implements PolarisEventListener {
  @Override
  public void onBeforeRequestRateLimited(BeforeRequestRateLimitedEvent event) {}

  @Override
  public void onBeforeTableCommit(BeforeTableCommitEvent event) {}

  @Override
  public void onAfterTableCommit(AfterTableCommitEvent event) {}

  @Override
  public void onBeforeViewCommit(BeforeViewCommitEvent event) {}

  @Override
  public void onAfterViewCommit(AfterViewCommitEvent event) {}

  @Override
  public void onBeforeRefreshTable(BeforeTableRefreshEvent event) {}

  @Override
  public void onAfterRefreshTable(AfterTableRefreshEvent event) {}

  @Override
  public void onBeforeRefreshView(BeforeViewRefreshEvent event) {}

  @Override
  public void onAfterRefreshView(AfterViewRefreshEvent event) {}

  @Override
  public void onBeforeAttemptTask(BeforeAttemptTaskEvent event) {}

  @Override
  public void onAfterAttemptTask(AfterAttemptTaskEvent event) {}
}
