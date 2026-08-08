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
package org.apache.polaris.extensions.lineage;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.Objects;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;

/** Creates lineage handlers for the current request. */
@RequestScoped
public class PolarisLineageHandlerFactory {
  private final LineageConfiguration configuration;
  private final CallContext callContext;

  @Inject
  public PolarisLineageHandlerFactory(LineageConfiguration configuration, CallContext callContext) {
    this.configuration = Objects.requireNonNull(configuration, "configuration must be non-null");
    this.callContext = Objects.requireNonNull(callContext, "callContext must be non-null");
  }

  public PolarisLineageHandler createHandler(PolarisPrincipal principal) {
    return new DefaultPolarisLineageHandler(configuration, callContext, principal);
  }
}
