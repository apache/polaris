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
package org.apache.polaris.service.quarkus.test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import org.hawkular.agent.prometheus.text.TextPrometheusMetricsProcessor;
import org.hawkular.agent.prometheus.types.MetricFamily;
import org.hawkular.agent.prometheus.walkers.CollectorPrometheusMetricsWalker;

/** Utils for working with metrics in tests */
public class TestMetricsUtil {

  public static Map<String, MetricFamily> fetchMetrics(
      Client client, URI baseManagementUri, String endpointPath) {
    Response response =
        client.target(String.format(endpointPath, baseManagementUri)).request().get();
    if (response.getStatus() == Status.MOVED_PERMANENTLY.getStatusCode()) {
      response = client.target(response.getLocation()).request().get();
    }
    assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    String body = response.readEntity(String.class);
    InputStream inputStream = new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
    CollectorPrometheusMetricsWalker walker = new CollectorPrometheusMetricsWalker();
    new TextPrometheusMetricsProcessor(inputStream, walker).walk();
    return walker.getAllMetricFamilies().stream()
        .collect(
            Collectors.toMap(
                MetricFamily::getName,
                metricFamily -> metricFamily,
                (mf1, mf2) -> {
                  throw new IllegalStateException("Duplicate metric family: " + mf1.getName());
                }));
  }
}
