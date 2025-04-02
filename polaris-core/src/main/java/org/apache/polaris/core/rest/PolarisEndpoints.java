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
package org.apache.polaris.core.rest;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.iceberg.rest.Endpoint;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;

public class PolarisEndpoints {
  public static final Endpoint V1_LIST_GENERIC_TABLES =
      Endpoint.create("GET", PolarisResourcePaths.V1_GENERIC_TABLES);
  public static final Endpoint V1_LOAD_GENERIC_TABLE =
      Endpoint.create("GET", PolarisResourcePaths.V1_GENERIC_TABLE);
  public static final Endpoint V1_CREATE_GENERIC_TABLE =
      Endpoint.create("POST", PolarisResourcePaths.V1_GENERIC_TABLES);
  public static final Endpoint V1_DELETE_GENERIC_TABLE =
      Endpoint.create("DELETE", PolarisResourcePaths.V1_GENERIC_TABLE);

  public static final Set<Endpoint> GENERIC_TABLE_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(V1_LIST_GENERIC_TABLES)
          .add(V1_CREATE_GENERIC_TABLE)
          .add(V1_DELETE_GENERIC_TABLE)
          .add(V1_LOAD_GENERIC_TABLE)
          .build();

  /**
   * Get the generic table endpoints. Returns GENERIC_TABLE_ENDPOINTS if ENABLE_GENERIC_TABLES is
   * set to true, otherwise, returns an empty set.
   */
  public static Set<Endpoint> getSupportedGenericTableEndpoints(CallContext callContext) {
    // add the generic table endpoints as supported endpoints if generic table feature is enabled.
    boolean genericTableEnabled =
        callContext
            .getPolarisCallContext()
            .getConfigurationStore()
            .getConfiguration(
                callContext.getPolarisCallContext(), FeatureConfiguration.ENABLE_GENERIC_TABLES);

    return genericTableEnabled ? PolarisEndpoints.GENERIC_TABLE_ENDPOINTS : ImmutableSet.of();
  }
}
