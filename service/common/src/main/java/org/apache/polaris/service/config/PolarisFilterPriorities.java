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
package org.apache.polaris.service.config;

import jakarta.ws.rs.Priorities;

public final class PolarisFilterPriorities {
  public static final int REALM_CONTEXT_FILTER = Priorities.AUTHENTICATION - 100;
  public static final int AUTHENTICATOR_FILTER = Priorities.AUTHENTICATION;
  public static final int ROLES_PROVIDER_FILTER = Priorities.AUTHENTICATION + 1;
  public static final int RATE_LIMITER_FILTER = Priorities.USER;
}
