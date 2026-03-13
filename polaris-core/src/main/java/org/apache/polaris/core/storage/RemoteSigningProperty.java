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

package org.apache.polaris.core.storage;

import org.apache.iceberg.rest.RESTCatalogProperties;

/**
 * Properties that Polaris passes down to the Iceberg remote signer client, and that the client must
 * send back to the Polaris signer endpoint for validation.
 */
public enum RemoteSigningProperty {

  /** The encrypted remote signing token, containing authorization information. Required. */
  TOKEN("polaris.remote-signing.token"),

  /**
   * Whether the signer should use path-style access for S3 URLs. Optional. Relevant only for S3.
   */
  PATH_STYLE_ACCESS("polaris.remote-signing.path-style-access");

  private final String shortName;
  private final String longName;

  RemoteSigningProperty(String shortName) {
    this.shortName = shortName;
    this.longName = RESTCatalogProperties.SIGNER_PROPERTIES_PREFIX + shortName;
  }

  /** The property name as it appears in signing requests. */
  public String shortName() {
    return shortName;
  }

  /** The property name as it appears in the load table response and in FileIO properties. */
  public String longName() {
    return longName;
  }
}
