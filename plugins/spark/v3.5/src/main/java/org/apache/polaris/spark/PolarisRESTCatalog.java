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

package org.apache.polaris.spark;

import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.polaris.core.rest.PolarisEndpoint;
import org.apache.polaris.core.rest.PolarisResourcePaths;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Set;
import java.util.logging.Logger;

public class PolarisRESTCatalog implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisRESTCatalog.class);

  private RESTClient restClient = null;
  private CloseableGroup closeables = null;
  private Set<Endpoint> endpoints;
  private OAuth2Util.AuthSession catalogAuth = null;
  private PolarisResourcePaths paths = null;

  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(PolarisEndpoint.V1_CREATE_GENERIC_ABLE)
          .add(PolarisEndpoint.V1_LOAD_GENERIC_TABLE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_DELETE_TABLE)
          .build();
}