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
package org.apache.polaris.service.catalog.tag;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.iceberg.rest.Endpoint;
import org.apache.polaris.core.rest.PolarisResourcePaths;

public class TagEndpoints {
  private TagEndpoints() {}

  public static final Endpoint V1_LIST_TAGS = Endpoint.create("GET", PolarisResourcePaths.V1_TAGS);
  public static final Endpoint V1_CREATE_TAG =
      Endpoint.create("POST", PolarisResourcePaths.V1_TAGS);
  public static final Endpoint V1_LOAD_TAG = Endpoint.create("GET", PolarisResourcePaths.V1_TAG);
  public static final Endpoint V1_UPDATE_TAG = Endpoint.create("PUT", PolarisResourcePaths.V1_TAG);
  public static final Endpoint V1_DROP_TAG = Endpoint.create("DELETE", PolarisResourcePaths.V1_TAG);

  public static final Set<Endpoint> TAG_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(V1_LIST_TAGS)
          .add(V1_CREATE_TAG)
          .add(V1_LOAD_TAG)
          .add(V1_UPDATE_TAG)
          .add(V1_DROP_TAG)
          .build();
}
