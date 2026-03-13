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
package org.apache.polaris.service.storage.sign;

import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * A remote signing token is an encrypted token that is passed from table endpoints to the signing
 * client, then back from the signing client to the signing endpoint.
 *
 * <p>These tokens contain pre-computed authorization information that allows the signing endpoint
 * to validate requests without re-computing authorization.
 *
 * <p>The token is encrypted using JWE with direct encryption to prevent tampering and ensure
 * confidentiality.
 */
@PolarisImmutable
public interface RemoteSigningToken {

  static ImmutableRemoteSigningToken.Builder builder() {
    return ImmutableRemoteSigningToken.builder();
  }

  /** The principal name that was authorized. */
  String principalName();

  /** The catalog name for which these parameters are valid. */
  String catalogName();

  /** The table identifier (namespace.table) for which these parameters are valid. */
  TableIdentifier tableIdentifier();

  /** The set of allowed locations that the user can access. */
  Set<StorageLocation> allowedLocations();

  /**
   * Whether the user is allowed to read and write data. If this is false, the user is only allowed
   * to read data.
   */
  boolean readWrite();

  /** Check that the token is valid. */
  @Value.Check
  default void check() {
    if (allowedLocations().isEmpty()) {
      throw new IllegalArgumentException("No allowed locations");
    }
  }
}
