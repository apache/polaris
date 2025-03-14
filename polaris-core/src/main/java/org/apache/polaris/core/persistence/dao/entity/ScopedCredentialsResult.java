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
package org.apache.polaris.core.persistence.dao.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.EnumMap;
import java.util.Map;
import org.apache.polaris.core.storage.PolarisCredentialProperty;

/** Result of a getSubscopedCredsForEntity() call */
public class ScopedCredentialsResult extends BaseResult {

  // null if not success. Else, set of name/value pairs for the credentials
  private final EnumMap<PolarisCredentialProperty, String> credentials;

  /**
   * Constructor for an error
   *
   * @param errorCode error code, cannot be SUCCESS
   * @param extraInformation extra information
   */
  public ScopedCredentialsResult(
      @Nonnull ReturnStatus errorCode, @Nullable String extraInformation) {
    super(errorCode, extraInformation);
    this.credentials = null;
  }

  /**
   * Constructor for success
   *
   * @param credentials credentials
   */
  public ScopedCredentialsResult(@Nonnull EnumMap<PolarisCredentialProperty, String> credentials) {
    super(ReturnStatus.SUCCESS);
    this.credentials = credentials;
  }

  @JsonCreator
  private ScopedCredentialsResult(
      @JsonProperty("returnStatus") @Nonnull ReturnStatus returnStatus,
      @JsonProperty("extraInformation") String extraInformation,
      @JsonProperty("credentials") Map<String, String> credentials) {
    super(returnStatus, extraInformation);
    this.credentials = new EnumMap<>(PolarisCredentialProperty.class);
    if (credentials != null) {
      credentials.forEach((k, v) -> this.credentials.put(PolarisCredentialProperty.valueOf(k), v));
    }
  }

  public EnumMap<PolarisCredentialProperty, String> getCredentials() {
    return credentials;
  }
}
