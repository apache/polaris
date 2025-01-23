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
package org.apache.polaris.service.it.env;

import java.io.Serializable;
import java.net.URI;

/**
 * This class contains the most fundamental information for accessing Polaris APIs, such as the base
 * URI and realm ID and provides methods for obtaining Icenberg REST API and Polaris Management
 * endpoints.
 */
public final class PolarisApiEndpoints implements Serializable {

  public static String REALM_HEADER = "realm";

  private final URI baseUri;
  private final String realm;

  public PolarisApiEndpoints(URI baseUri, String realm) {
    this.baseUri = baseUri;
    this.realm = realm;
  }

  public URI catalogApiEndpoint() {
    return baseUri.resolve("api/catalog");
  }

  public URI managementApiEndpoint() {
    return baseUri.resolve("api/management");
  }

  public String realm() {
    return realm;
  }
}
