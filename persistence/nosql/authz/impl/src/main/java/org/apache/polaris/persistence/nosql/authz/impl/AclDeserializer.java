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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import org.apache.polaris.persistence.nosql.authz.api.Acl;
import org.apache.polaris.persistence.nosql.authz.api.AclEntry;

class AclDeserializer extends JsonDeserializer<Acl> {
  @Override
  public Acl deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    if (p.currentToken() != JsonToken.START_OBJECT) {
      throw new JsonMappingException(p, "Unexpected token " + p.currentToken());
    }

    var privileges = JacksonPrivilegesModule.currentPrivileges();
    var builder = AclImpl.builder(privileges);
    for (var t = p.nextToken(); t != JsonToken.END_OBJECT; t = p.nextToken()) {
      if (t == JsonToken.FIELD_NAME) {
        var roleId = p.currentName();
        p.nextToken();
        var entry = p.readValueAs(AclEntry.class);
        builder.addEntry(roleId, entry);
      }
    }
    return builder.build();
  }
}
