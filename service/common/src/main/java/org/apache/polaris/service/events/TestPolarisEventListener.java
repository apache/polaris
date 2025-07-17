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

import com.google.common.collect.Streams;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.context.CallContext;

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
  public void onBeforeRequestRateLimited(BeforeRequestRateLimitedEvent event) {
    history.add(event);
  }

  @Override
  public void onBeforeTableCommited(
      BeforeTableCommitedEvent event, CallContext callCtx, SecurityContext securityContext) {
    history.add(event);
  }

  @Override
  public void onAfterTableCommited(
      AfterTableCommitedEvent event, CallContext callCtx, SecurityContext securityContext) {
    history.add(event);
  }

  @Override
  public void onBeforeViewCommited(
      BeforeViewCommitedEvent event, CallContext callCtx, SecurityContext securityContext) {
    history.add(event);
  }

  @Override
  public void onAfterViewCommited(
      AfterViewCommitedEvent event, CallContext callCtx, SecurityContext securityContext) {
    history.add(event);
  }

  @Override
  public void onBeforeTableRefreshed(
      BeforeTableRefreshedEvent event, CallContext callCtx, SecurityContext securityContext) {
    history.add(event);
  }

  @Override
  public void onAfterTableRefreshed(
      AfterTableRefreshedEvent event, CallContext callCtx, SecurityContext securityContext) {
    history.add(event);
  }

  @Override
  public void onBeforeViewRefreshed(
      BeforeViewRefreshedEvent event, CallContext callCtx, SecurityContext securityContext) {
    history.add(event);
  }

  @Override
  public void onAfterViewRefreshed(
      AfterViewRefreshedEvent event, CallContext callCtx, SecurityContext securityContext) {
    history.add(event);
  }

  @Override
  public void onBeforeTaskAttempted(BeforeTaskAttemptedEvent event) {
    history.add(event);
  }

  @Override
  public void onAfterTaskAttempted(AfterTaskAttemptedEvent event) {
    history.add(event);
  }
}
