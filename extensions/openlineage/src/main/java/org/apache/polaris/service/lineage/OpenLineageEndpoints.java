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
package org.apache.polaris.service.lineage;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.iceberg.rest.Endpoint;

/**
 * Endpoints advertised for the OpenLineage ingest API. These mirror the paths mounted by {@code
 * PolarisOpenLineageApi} ({@code /api/openlineage/v1/lineage} and {@code
 * /api/openlineage/v1/lineage/batch}).
 */
public class OpenLineageEndpoints {
  private OpenLineageEndpoints() {}

  public static final Endpoint V1_SEND_LINEAGE_EVENT =
      Endpoint.create("POST", "/api/openlineage/v1/lineage");
  public static final Endpoint V1_SEND_LINEAGE_EVENT_BATCH =
      Endpoint.create("POST", "/api/openlineage/v1/lineage/batch");

  public static final Set<Endpoint> OPENLINEAGE_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(V1_SEND_LINEAGE_EVENT)
          .add(V1_SEND_LINEAGE_EVENT_BATCH)
          .build();
}
