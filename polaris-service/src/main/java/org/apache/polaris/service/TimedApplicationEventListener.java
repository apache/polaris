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
package org.apache.polaris.service;

import static org.apache.polaris.core.monitor.PolarisMetricRegistry.TAG_RESP_CODE;

import com.google.common.base.Stopwatch;
import io.micrometer.core.instrument.Tag;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.ext.Provider;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.monitor.PolarisMetricRegistry;
import org.apache.polaris.core.resource.TimedApi;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * An ApplicationEventListener that supports timing and error counting of Jersey resource methods
 * annotated by {@link TimedApi}. It uses the {@link PolarisMetricRegistry} for metric collection
 * and properly times the resource on success and increments the error counter on failure.
 */
@Provider
public class TimedApplicationEventListener implements ApplicationEventListener {

  /**
   * Each API will increment a common counter (SINGLETON_METRIC_NAME) but have its API name tagged
   * (TAG_API_NAME).
   */
  public static final String SINGLETON_METRIC_NAME = "polaris.api";

  public static final String TAG_API_NAME = "api_name";

  // The PolarisMetricRegistry instance used for recording metrics and error counters.
  private final PolarisMetricRegistry polarisMetricRegistry;

  public TimedApplicationEventListener(PolarisMetricRegistry polarisMetricRegistry) {
    this.polarisMetricRegistry = polarisMetricRegistry;
  }

  @VisibleForTesting
  public PolarisMetricRegistry getMetricRegistry() {
    return polarisMetricRegistry;
  }

  @Override
  public void onEvent(ApplicationEvent event) {}

  @Override
  public RequestEventListener onRequest(RequestEvent event) {
    return new TimedRequestEventListener();
  }

  /**
   * A RequestEventListener implementation that handles timing of resource method execution and
   * increments error counters on failures. The lifetime of the listener is tied to a single HTTP
   * request.
   */
  private class TimedRequestEventListener implements RequestEventListener {
    private String metric;
    private Stopwatch sw;

    /** Handles various types of RequestEvents to start timing, stop timing, and record metrics. */
    @Override
    public void onEvent(RequestEvent event) {
      String realmId = CallContext.getCurrentContext().getRealmContext().getRealmIdentifier();
      if (event.getType() == RequestEvent.Type.REQUEST_MATCHED) {
        Method method =
            event.getUriInfo().getMatchedResourceMethod().getInvocable().getHandlingMethod();
        if (method.isAnnotationPresent(TimedApi.class)) {
          TimedApi timedApi = method.getAnnotation(TimedApi.class);
          metric = timedApi.value();

          // Increment both the counter with the API name in the metric name and a common metric
          polarisMetricRegistry.incrementCounter(metric, realmId);
          polarisMetricRegistry.incrementCounter(
              SINGLETON_METRIC_NAME, realmId, Tag.of(TAG_API_NAME, metric));
        }
      } else if (event.getType() == RequestEvent.Type.RESOURCE_METHOD_START && metric != null) {
        sw = Stopwatch.createStarted();
      } else if (event.getType() == RequestEvent.Type.FINISHED && metric != null) {
        if (event.isSuccess()) {
          sw.stop();
          polarisMetricRegistry.recordTimer(metric, sw.elapsed(TimeUnit.MILLISECONDS), realmId);
        } else {
          int statusCode = event.getContainerResponse().getStatus();

          // Increment both the counter with the API name in the metric name and a common metric
          polarisMetricRegistry.incrementErrorCounter(metric, statusCode, realmId);
          polarisMetricRegistry.incrementErrorCounter(
              SINGLETON_METRIC_NAME,
              realmId,
              List.of(
                  Tag.of(TAG_API_NAME, metric), Tag.of(TAG_RESP_CODE, String.valueOf(statusCode))));
        }
      }
    }
  }
}
