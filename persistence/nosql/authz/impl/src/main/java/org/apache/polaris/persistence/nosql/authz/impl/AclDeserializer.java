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
import org.apache.polaris.persistence.nosql.authz.api.Acl;
import org.apache.polaris.persistence.nosql.authz.api.AclEntry;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DatabindException;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;

class AclDeserializer extends ValueDeserializer<Acl> {
  private final Supplier<Privileges> privilegesResolver;

  AclDeserializer(Supplier<Privileges> privilegesResolver) {
    this.privilegesResolver =
        requireNonNull(privilegesResolver, "privilegesResolver must not be null");
  }

  @Override
  public Acl deserialize(JsonParser p, DeserializationContext ctxt) {
    if (p.currentToken() != JsonToken.START_OBJECT) {
      throw DatabindException.from(p, "Unexpected token " + p.currentToken());
    }

    var privileges = privilegesResolver.get();
    var builder = AclImpl.builder(privileges);
    for (var t = p.nextToken(); t != JsonToken.END_OBJECT; t = p.nextToken()) {
      if (t == JsonToken.PROPERTY_NAME) {
        var roleId = p.currentName();
        p.nextToken();
        var entry = p.readValueAs(AclEntry.class);
        builder.addEntry(roleId, entry);
      }
    }
    return builder.build();
  }
}
