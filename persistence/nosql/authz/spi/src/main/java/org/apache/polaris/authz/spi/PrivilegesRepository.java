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
package org.apache.polaris.authz.spi;

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.authz.api.Privilege;

/**
 * API to maintain the Polaris system-wide mapping of {@linkplain Privilege privilege} {@linkplain
 * Privilege#name() names} to and from integer IDs.
 *
 * <p>Implementation is provided as an {@link ApplicationScoped @ApplicationScoped} bean.
 */
public interface PrivilegesRepository {
  @Nonnull
  PrivilegesMapping fetchPrivilegesMapping();

  boolean updatePrivilegesMapping(
      @Nonnull PrivilegesMapping expectedState, @Nonnull PrivilegesMapping newState);
}
