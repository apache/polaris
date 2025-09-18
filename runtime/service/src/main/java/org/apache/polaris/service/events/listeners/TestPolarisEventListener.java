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

import com.google.common.collect.Streams;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.service.events.AfterAttemptTaskEvent;
import org.apache.polaris.service.events.BeforeAttemptTaskEvent;
import org.apache.polaris.service.events.BeforeLimitRequestRateEvent;
import org.apache.polaris.service.events.IcebergRestCatalogEvents;
import org.apache.polaris.service.events.PolarisEvent;

/** Event listener that stores all emitted events forever. Not recommended for use in production. */
@ApplicationScoped
@Identifier("test")
public class TestPolarisEventListener extends PolarisEventListener {
  private final List<PolarisEvent> history = new ArrayList<>();

  public <T> T getLatest(Class<T> type) {
    return (T)
        Streams.findLast(history.stream().filter(type::isInstance)).map(type::cast).orElseThrow();
  }

  @Override
  public void onBeforeLimitRequestRate(BeforeLimitRequestRateEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCommitTable(IcebergRestCatalogEvents.BeforeCommitTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCommitTable(IcebergRestCatalogEvents.AfterCommitTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeCommitView(IcebergRestCatalogEvents.BeforeCommitViewEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterCommitView(IcebergRestCatalogEvents.AfterCommitViewEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeRefreshTable(IcebergRestCatalogEvents.BeforeRefreshTableEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterRefreshTable(IcebergRestCatalogEvents.AfterRefreshTableEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeRefreshView(IcebergRestCatalogEvents.BeforeRefreshViewEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterRefreshView(IcebergRestCatalogEvents.AfterRefreshViewEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeAttemptTask(BeforeAttemptTaskEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterAttemptTask(AfterAttemptTaskEvent event) {
    history.add(event);
  }
}
