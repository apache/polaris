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
package org.apache.polaris.service.test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.micrometer.core.instrument.Tag;
import jakarta.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.polaris.service.config.PolarisApplicationConfig;

/** Utils for working with metrics in tests */
public class TestMetricsUtil {
  private static final String SUFFIX_TOTAL = ".total";

  /** Gets a total counter by calling the Prometheus metrics endpoint */
  public static double getTotalCounter(
      DropwizardAppExtension<PolarisApplicationConfig> dropwizardAppExtension,
      String metricName,
      Collection<Tag> tags) {

    metricName += SUFFIX_TOTAL;
    metricName = metricName.replace('.', '_').replace('-', '_');

    // Example of a line from the metrics endpoint:
    // polaris_TimedApi_count_realm_total{API_NAME="polaris.principals.getPrincipal",REALM_ID="org_apache_polaris_service_TimedApplicationEventListenerTest"} 1.0
    // This method assumes that tag ordering isn't guaranteed
    List<String> tagFilters =
        tags.stream().map(tag -> String.format("%s=\"%s\"", tag.getKey(), tag.getValue())).toList();

    Response response =
        dropwizardAppExtension
            .client()
            .target(
                String.format("http://localhost:%d/metrics", dropwizardAppExtension.getAdminPort()))
            .request()
            .get();
    assertThat(response).returns(Response.Status.OK.getStatusCode(), Response::getStatus);
    String[] responseLines = response.readEntity(String.class).split("\n");
    for (String line : responseLines) {
      int numTags =
          StringUtils.countMatches(line, '='); // Assumes the tag values don't contain an '='
      if (line.startsWith(metricName)
          && tagFilters.stream().allMatch(line::contains)
          && numTags == tagFilters.size()) {
        return Double.parseDouble(line.split(" ")[1]);
      }
    }
    return 0;
  }
}
