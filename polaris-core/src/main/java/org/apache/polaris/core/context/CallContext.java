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
package org.apache.polaris.core.context;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.RealmConfig;

/**
 * Stores elements associated with an individual REST request such as RealmContext, caller
 * identity/role, authn/authz, etc. This class is distinct from RealmContext because implementations
 * may need to first independently resolve a RealmContext before resolving the identity/role
 * elements of the CallContext that reside exclusively within the resolved Realm. For example, the
 * principal/role entities may be defined within a Realm-specific persistence layer, and the
 * underlying nature of the persistence layer may differ between different realms.
 */
public interface CallContext {
  InheritableThreadLocal<CallContext> CURRENT_CONTEXT = new InheritableThreadLocal<>();

  static CallContext setCurrentContext(CallContext context) {
    CURRENT_CONTEXT.set(context);
    return context;
  }

  static CallContext getCurrentContext() {
    return CURRENT_CONTEXT.get();
  }

  static void unsetCurrentContext() {
    CURRENT_CONTEXT.remove();
  }

  /** Copy the {@link CallContext}. */
  CallContext copy();

  RealmContext getRealmContext();

  /**
   * @return the inner context used for delegating services
   */
  PolarisCallContext getPolarisCallContext();

  RealmConfig getRealmConfig();
}
