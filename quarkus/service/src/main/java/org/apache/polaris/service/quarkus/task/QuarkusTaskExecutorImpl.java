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
package org.apache.polaris.service.quarkus.task;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.quarkus.runtime.Startup;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.service.quarkus.tracing.QuarkusTracingFilter;
import org.apache.polaris.service.task.TaskExecutorImpl;
import org.apache.polaris.service.task.TaskFileIOSupplier;

@ApplicationScoped
public class QuarkusTaskExecutorImpl extends TaskExecutorImpl {

  private final Tracer tracer;

  public QuarkusTaskExecutorImpl() {
    this(null, null, null, null, null, null, null);
  }

  @Inject
  public QuarkusTaskExecutorImpl(
      @Identifier("task-executor") ExecutorService executorService,
      MetaStoreManagerFactory metaStoreManagerFactory,
      PolarisConfigurationStore configurationStore,
      PolarisDiagnostics diagnostics,
      TaskFileIOSupplier fileIOSupplier,
      Clock clock,
      Tracer tracer) {
    super(
        executorService,
        metaStoreManagerFactory,
        configurationStore,
        diagnostics,
        fileIOSupplier,
        clock);
    this.tracer = tracer;
  }

  @Startup
  @Override
  public void init() {
    super.init();
  }

  @Override
  protected void handleTask(long taskEntityId, RealmContext realmContext, int attempt) {
    Span span =
        tracer
            .spanBuilder("polaris.task")
            .setParent(Context.current())
            .setAttribute(
                QuarkusTracingFilter.REALM_ID_ATTRIBUTE, realmContext.getRealmIdentifier())
            .setAttribute("polaris.task.entity.id", taskEntityId)
            .setAttribute("polaris.task.attempt", attempt)
            .startSpan();
    try (Scope ignored = span.makeCurrent()) {
      super.handleTask(taskEntityId, realmContext, attempt);
    } finally {
      span.end();
    }
  }
}
