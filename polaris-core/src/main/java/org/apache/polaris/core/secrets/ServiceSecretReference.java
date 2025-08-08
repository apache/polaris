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

package org.apache.polaris.core.secrets;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;

/**
 * Represents a "wrapped reference" to a service-owned secret that holds an identifier to retrieve
 * possibly remotely-stored secret material, along with an open-ended "referencePayload" that is
 * specific to an implementation of the secret storage and which is needed "unwrap" the actual
 * secret in combination with whatever is stored in the remote secrets storage.
 */
public class ServiceSecretReference extends UserSecretReference {
  /**
   * @param urn A string which should be self-sufficient to retrieve whatever secret material that
   *     is stored in the remote secret store. e.g.,
   *     'urn:polaris-service-secret:&lt;service-manager-type&gt;:&lt;type-specific-identifier&gt;
   * @param referencePayload Optionally, any additional information that is necessary to fully
   *     reconstitute the original secret based on what is retrieved by the {@code urn}; this
   *     payload may include hashes/checksums, encryption key ids, OTP encryption keys, additional
   *     protocol/version specifiers, etc., which are implementation-specific.
   */
  public ServiceSecretReference(
      @JsonProperty(value = "urn", required = true) @Nonnull String urn,
      @JsonProperty(value = "referencePayload") @Nullable Map<String, String> referencePayload) {
    super(urn, referencePayload);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof ServiceSecretReference && super.equals(obj);
  }
}
