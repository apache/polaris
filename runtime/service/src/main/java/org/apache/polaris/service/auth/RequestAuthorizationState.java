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
package org.apache.polaris.service.auth;

import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.RequestScoped;
import java.util.Objects;
import org.apache.polaris.core.auth.AuthorizationState;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;

@RequestScoped
public class RequestAuthorizationState implements AuthorizationState {
  private PolarisResolutionManifest resolutionManifest;

  @Override
  public @Nonnull PolarisResolutionManifest getResolutionManifest() {
    return Objects.requireNonNull(resolutionManifest, "resolutionManifest");
  }

  @Override
  public void setResolutionManifest(@Nonnull PolarisResolutionManifest resolutionManifest) {
    this.resolutionManifest = Objects.requireNonNull(resolutionManifest, "resolutionManifest");
  }
}
