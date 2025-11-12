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

import static org.apache.polaris.persistence.nosql.authz.impl.JacksonPrivilegesModule.currentPrivileges;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import org.apache.polaris.persistence.nosql.authz.api.PrivilegeSet;

public class PrivilegeSetSerializer extends JsonSerializer<PrivilegeSet> {
  @Override
  public void serialize(PrivilegeSet value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    var view = serializers.getActiveView();
    if (view != null && view.getSimpleName().equals("StorageView")) {
      // When serializing for/to persistence as a use the bit-encoded/binary
      // serialization. This is triggered when the current Jackson view is
      // `org.apache.polaris.persistence.nosql.api.obj.Obj.StorageView`.
      if (!value.isEmpty()) {
        var impl = (PrivilegeSetImpl) value;
        gen.writeBinary(impl.bytesUnsafe());
      } else {
        gen.writeNull();
      }
    } else {
      // Otherwise, for external/REST, use the privilege names from the "global" set of
      // privileges.
      gen.writeStartArray();
      var collapsed = value.collapseComposites(currentPrivileges());
      for (var privilege : collapsed) {
        gen.writeString(privilege.name());
      }
      gen.writeEndArray();
    }
  }
}
