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

import java.util.List;
import java.util.Optional;
import org.jspecify.annotations.NonNull;

/**
 * SPI for a storage integration bound to a particular storage configuration. An integration vends
 * scoped storage credentials for requests against its configured backend.
 *
 * <p>Implementations are returned by {@link PolarisStorageIntegrationProvider} given a resolved
 * entity path; the provider decides how an integration instance is created and cached. The default
 * cloud integrations (AWS, GCP, Azure) extend {@link CachingStorageIntegration}, which adds
 * in-memory caching of vended credentials. Other implementations — e.g. persistence-backed
 * credential pools — may implement this interface directly without extending the caching base
 * class.
 */
public interface PolarisStorageIntegration {

  /**
   * Vend a scoped {@link StorageAccessConfig} for the given list of {@link LocationGrant}s.
   *
   * <p>The AWS and GCP implementations honor per-grant action separation: a grant of {@code (loc,
   * {WRITE})} does not cause {@code loc} to receive read or list permissions in the resulting
   * credentials. The Azure implementation cannot fully honor per-grant per-prefix separation
   * because a SAS token is monolithic at the container (or, for hierarchical ADLS, single-path)
   * level; its action flags reflect the union of requested actions across all grants.
   *
   * @param grants per-location action requests; each grant pairs a set of storage location URIs
   *     with the operations (READ, WRITE, LIST, DELETE, ALL) the credentials should permit on those
   *     locations
   * @param refreshEndpoint optional endpoint URL for clients to refresh credentials
   * @param context metadata (catalog, principal, roles, trace id, etc.) attached to the vending
   *     call — used for audit tagging and cache keying
   */
  StorageAccessConfig getStorageAccessConfig(
      @NonNull List<LocationGrant> grants,
      @NonNull Optional<String> refreshEndpoint,
      @NonNull CredentialVendingContext context);

  /**
   * Hook invoked before a table is created to pre-materialize storage locations that the underlying
   * storage system requires to exist before files can be written into them.
   *
   * <p>The default is a no-op. Storage types that need explicit folder creation (e.g. GCS with
   * Hierarchical Namespace enabled) override this. Passed locations include the table location and
   * its metadata/data subpaths.
   */
  default void prepareLocations(@NonNull List<String> locations) {}
}
