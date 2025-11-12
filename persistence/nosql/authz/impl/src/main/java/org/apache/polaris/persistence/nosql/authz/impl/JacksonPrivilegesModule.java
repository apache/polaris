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

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.module.SimpleModule;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.CDI;
import java.util.function.Function;
import org.apache.polaris.persistence.nosql.authz.api.Acl;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeSet;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;

public class JacksonPrivilegesModule extends SimpleModule {
  public JacksonPrivilegesModule() {
    addDeserializer(PrivilegeSet.class, new PrivilegeSetDeserializer());
    addSerializer(PrivilegeSet.class, new PrivilegeSetSerializer());
    addDeserializer(Acl.class, new AclDeserializer());
    addSerializer(Acl.class, new AclSerializer());
  }

  static Privileges currentPrivileges() {
    return cdiResolve(Privileges.class);
  }

  // TODO the following is the same as in AbstractTypeIdResolver

  /**
   * Resolve the given type via {@link CDI#current() CDI.current()}. For tests the resolution via
   * CDI can be {@linkplain CDIResolver#setResolver(Function) routed to a custom function}.
   */
  private static <R> R cdiResolve(Class<R> type) {
    // TODO instead of doing the 'CDIResolver' dance, we could (should?) have an attribute in the
    //  `DatabindContext` holding a reference to the CDI instance (referred to as `Instance<?>`).
    var resolved = CDIResolver.resolver.apply(type);
    @SuppressWarnings("unchecked")
    var r = (R) resolved;
    return r;
  }

  public static final class CDIResolver {
    static Function<Class<?>, ?> resolver = CDIResolver::resolveViaCurrentCDI;

    /**
     * The helper function {@link #cdiResolve(Class)} is used by {@link JacksonPrivilegesModule}
     * implementations to resolve the {@link Privileges} instance, and the default implementation of
     * {@link #cdiResolve(Class)} relies on {@link CDI#current() CDI.current()} to resolve against a
     * "singleton" {@link CDI} instance. Some tests do not use CDI. Setting a custom resolver
     * function helps in such scenarios.
     */
    @SuppressWarnings("unused")
    public static void setResolver(Function<Class<?>, ?> resolver) {
      CDIResolver.resolver = resolver;
    }

    /**
     * Manually reset a custom {@linkplain #setResolver(Function) CDI resolver}. This is usually
     * performed automatically after each test case.
     */
    @SuppressWarnings("unused")
    public static void resetResolver() {
      resolver = CDIResolver::resolveViaCurrentCDI;
    }

    private static Object resolveViaCurrentCDI(Class<?> type) {
      Instance<?> selected = CDI.current().select(type);
      checkArgument(selected.isResolvable(), "Cannot resolve %s", type);
      checkArgument(!selected.isAmbiguous(), "Ambiguous type %s", type);
      return selected.get();
    }
  }
}
