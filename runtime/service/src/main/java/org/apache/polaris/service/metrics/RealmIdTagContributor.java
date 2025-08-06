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
package org.apache.polaris.service.metrics;

import io.micrometer.core.instrument.Tags;
import io.quarkus.micrometer.runtime.HttpServerMetricsTagsContributor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.context.RealmContextFilter;

@ApplicationScoped
public class RealmIdTagContributor implements HttpServerMetricsTagsContributor {

  public static final String TAG_REALM = "realm_id";

  @Inject MetricsConfiguration metricsConfiguration;

  @Override
  public Tags contribute(Context context) {
    if (!metricsConfiguration.realmIdTag().enableInHttpMetrics()) {
      return Tags.empty();
    }
    RealmContext realmContext =
        context.requestContextLocalData(RealmContextFilter.REALM_CONTEXT_KEY);
    return Tags.of(TAG_REALM, realmContext.getRealmIdentifier());
  }
}
