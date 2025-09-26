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
package org.apache.polaris.persistence.nosql.coretypes.changes;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import java.io.IOException;
import java.util.Map;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * "Generic" change, only to be used as a fallback when hitting an unknown change type during
 * deserialization.
 */
@PolarisImmutable
@JsonSerialize(using = GenericChange.GenericChangeInfoSerializer.class)
@JsonDeserialize(using = GenericChange.GenericChangeInfoDeserializer.class)
public interface GenericChange extends Change {
  @Override
  @Value.Parameter
  ChangeType getType();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonUnwrapped
  @Value.Parameter
  Map<String, Object> getAttributes();

  final class GenericChangeInfoSerializer extends JsonSerializer<GenericChange> {

    @Override
    public void serializeWithType(
        GenericChange value,
        JsonGenerator gen,
        SerializerProvider serializers,
        TypeSerializer typeSer)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField("type", value.getType().name());
      var attributes = value.getAttributes();
      if (attributes != null) {
        for (var entry : attributes.entrySet()) {
          gen.writeFieldName(entry.getKey());
          gen.writeObject(entry.getValue());
        }
      }
      gen.writeEndObject();
    }

    @Override
    public void serialize(GenericChange value, JsonGenerator gen, SerializerProvider serializers) {
      throw new UnsupportedOperationException();
    }
  }

  final class GenericChangeInfoDeserializer extends JsonDeserializer<GenericChange> {

    @SuppressWarnings("unchecked")
    @Override
    public GenericChange deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      var all = p.readValueAs(Map.class);
      var type = (String) all.remove("type");
      return ImmutableGenericChange.of(ChangeType.valueOf(type), all);
    }
  }
}
