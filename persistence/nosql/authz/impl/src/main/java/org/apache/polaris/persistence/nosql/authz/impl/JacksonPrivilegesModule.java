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
package org.apache.polaris.persistence.nosql.authz.impl;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.module.SimpleModule;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.authz.api.Acl;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeSet;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;

public class JacksonPrivilegesModule extends SimpleModule {
  @SuppressWarnings("unused")
  public JacksonPrivilegesModule() {
    this(PrivilegesViaCDI::privileges);
  }

  JacksonPrivilegesModule(Supplier<Privileges> privilegesResolver) {
    requireNonNull(privilegesResolver, "privilegesResolver must not be null");
    addDeserializer(PrivilegeSet.class, new PrivilegeSetDeserializer(privilegesResolver));
    addSerializer(PrivilegeSet.class, new PrivilegeSetSerializer(privilegesResolver));
    addDeserializer(Acl.class, new AclDeserializer(privilegesResolver));
    addSerializer(Acl.class, new AclSerializer());
  }

  /**
   * Helper to memoize the {@link Privileges} instance in a CDI environment. This class bridges the
   * gap between CDI and Jackson module lifecycles.
   *
   * <p>The implementation attempts to infer the {@link Privileges} instance from the CDI container
   * by observing the {@link Startup} event. However, other beans that also observe the {@link
   * Startup} event can trigger a call to the {@link #privileges()} method before this bean's {@link
   * #init(Startup)} function is called.
   */
  @ApplicationScoped
  static class PrivilegesViaCDI {
    @Inject Privileges privileges;

    private static volatile Privileges privilegesForJackson;

    void init(@Observes Startup startup) {
      checkState(
          privilegesForJackson == null || privilegesForJackson == privileges,
          "A CDI container has already been started");
      privilegesForJackson = privileges;
    }

    void dispose(@Observes Shutdown shutdown) {
      privilegesForJackson = null;
    }

    static Privileges privileges() {
      var privs = privilegesForJackson;
      if (privs != null) {
        // fast path
        return privs;
      }
      // slow path
      privs = CDI.current().select(Privileges.class).get();
      privilegesForJackson = privs;
      return privs;
    }
  }
}
