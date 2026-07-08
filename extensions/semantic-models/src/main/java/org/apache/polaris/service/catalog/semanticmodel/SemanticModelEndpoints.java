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
package org.apache.polaris.service.catalog.semanticmodel;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.iceberg.rest.Endpoint;
import org.apache.polaris.core.rest.PolarisResourcePaths;

public class SemanticModelEndpoints {
  private SemanticModelEndpoints() {}

  public static final Endpoint V1_LIST_SEMANTIC_MODELS =
      Endpoint.create("GET", PolarisResourcePaths.V1_SEMANTIC_MODELS);
  public static final Endpoint V1_CREATE_SEMANTIC_MODEL =
      Endpoint.create("POST", PolarisResourcePaths.V1_SEMANTIC_MODELS);
  public static final Endpoint V1_LOAD_SEMANTIC_MODEL =
      Endpoint.create("GET", PolarisResourcePaths.V1_SEMANTIC_MODEL);
  public static final Endpoint V1_UPDATE_SEMANTIC_MODEL =
      Endpoint.create("PUT", PolarisResourcePaths.V1_SEMANTIC_MODEL);
  public static final Endpoint V1_DROP_SEMANTIC_MODEL =
      Endpoint.create("DELETE", PolarisResourcePaths.V1_SEMANTIC_MODEL);

  public static final Set<Endpoint> SEMANTIC_MODEL_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(V1_LIST_SEMANTIC_MODELS)
          .add(V1_CREATE_SEMANTIC_MODEL)
          .add(V1_LOAD_SEMANTIC_MODEL)
          .add(V1_UPDATE_SEMANTIC_MODEL)
          .add(V1_DROP_SEMANTIC_MODEL)
          .build();
}
