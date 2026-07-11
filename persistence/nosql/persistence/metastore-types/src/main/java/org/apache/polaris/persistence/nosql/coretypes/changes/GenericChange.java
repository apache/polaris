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
import java.util.Map;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonSerialize;
import tools.jackson.databind.jsontype.TypeSerializer;

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

  final class GenericChangeInfoSerializer extends ValueSerializer<GenericChange> {

    @Override
    public void serializeWithType(
        GenericChange value,
        JsonGenerator gen,
        SerializationContext serializationContext,
        TypeSerializer typeSer) {
      gen.writeStartObject();
      gen.writeStringProperty("type", value.getType().name());
      var attributes = value.getAttributes();
      if (attributes != null) {
        for (var entry : attributes.entrySet()) {
          gen.writePOJOProperty(entry.getKey(), entry.getValue());
        }
      }
      gen.writeEndObject();
    }

    @Override
    public void serialize(
        GenericChange value, JsonGenerator gen, SerializationContext serializationContext) {
      throw new UnsupportedOperationException();
    }
  }

  final class GenericChangeInfoDeserializer extends ValueDeserializer<GenericChange> {

    @SuppressWarnings("unchecked")
    @Override
    public GenericChange deserialize(JsonParser p, DeserializationContext ctxt) {
      var all = p.readValueAs(Map.class);
      var type = (String) all.remove("type");
      return ImmutableGenericChange.of(ChangeType.valueOf(type), all);
    }
  }
}
