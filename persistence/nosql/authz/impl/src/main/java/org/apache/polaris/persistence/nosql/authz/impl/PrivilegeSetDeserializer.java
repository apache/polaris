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

import static java.util.Objects.requireNonNull;

import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeSet;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DatabindException;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;

class PrivilegeSetDeserializer extends ValueDeserializer<PrivilegeSet> {
  private final Supplier<Privileges> privilegesResolver;

  PrivilegeSetDeserializer(Supplier<Privileges> privilegesResolver) {
    this.privilegesResolver =
        requireNonNull(privilegesResolver, "privilegesResolver must not be null");
  }

  @Override
  public PrivilegeSet deserialize(JsonParser p, DeserializationContext ctxt) {
    switch (p.currentToken()) {
      case VALUE_NULL:
        return new PrivilegeSetImpl(privilegesResolver.get(), new byte[0]);
      case VALUE_STRING:
      case VALUE_EMBEDDED_OBJECT:
        // Internal, storage serialization format.
        var bytes = p.getBinaryValue();
        return new PrivilegeSetImpl(privilegesResolver.get(), bytes);
      case START_ARRAY:
        // External/REST serialization format using privilege names.
        var privileges = privilegesResolver.get();
        var builder = PrivilegeSetImpl.builder(privileges);
        for (var t = p.nextToken(); ; t = p.nextToken()) {
          // Note: switch(t) lets checkstyle fail
          if (t == JsonToken.END_ARRAY) {
            break;
          }
          if (t != JsonToken.VALUE_STRING) {
            throw DatabindException.from(p, "Unexpected JSON token " + t + " in privilege array");
          }
          builder.addPrivilege(privileges.byName(p.getString()));
        }
        return builder.build();
      default:
        throw DatabindException.from(p, "Unexpected JSON token " + p.currentToken());
    }
  }
}
