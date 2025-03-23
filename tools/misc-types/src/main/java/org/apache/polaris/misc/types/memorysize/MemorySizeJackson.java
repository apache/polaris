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
package org.apache.polaris.misc.types.memorysize;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.ContextualSerializer;
import java.io.IOException;

public class MemorySizeJackson extends SimpleModule {
  public MemorySizeJackson() {
    addDeserializer(MemorySize.class, new MemorySizeDeserializer());
    addSerializer(MemorySize.class, MemorySizeSerializer.AS_STRING);
  }

  private static class MemorySizeDeserializer extends JsonDeserializer<MemorySize> {
    @Override
    public MemorySize deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      switch (p.currentToken()) {
        case VALUE_NUMBER_INT:
          var bigInt = p.getBigIntegerValue();
          try {
            return new MemorySize.MemorySizeLong(bigInt.longValueExact());
          } catch (ArithmeticException e) {
            return new MemorySize.MemorySizeBig(bigInt);
          }
        case VALUE_STRING:
          return MemorySize.valueOf(p.getText());
        default:
          throw new IllegalArgumentException(
              "Unsupported token " + p.currentToken() + " for " + MemorySize.class.getName());
      }
    }
  }

  private static class MemorySizeSerializer extends JsonSerializer<MemorySize>
      implements ContextualSerializer {
    final boolean asInt;

    static final MemorySizeSerializer AS_STRING = new MemorySizeSerializer(false);
    static final MemorySizeSerializer AS_INT = new MemorySizeSerializer(true);

    private MemorySizeSerializer(boolean asInt) {
      this.asInt = asInt;
    }

    @Override
    public void serialize(MemorySize value, JsonGenerator generator, SerializerProvider serializers)
        throws IOException {
      if (asInt) {
        if (value instanceof MemorySize.MemorySizeBig) {
          generator.writeNumber(value.asBigInteger());
        } else {
          generator.writeNumber(value.asLong());
        }
      } else {
        generator.writeString(value.toString());
      }
    }

    @Override
    public JsonSerializer<?> createContextual(SerializerProvider provider, BeanProperty property) {

      if (property != null) {
        var propertyFormat = property.findPropertyFormat(provider.getConfig(), handledType());
        if (propertyFormat != null) {
          var shape = propertyFormat.getShape();
          switch (shape) {
            case NUMBER:
            case NUMBER_INT:
              return AS_INT;
            case STRING:
            case ANY:
            case NATURAL:
              return AS_STRING;
            default:
              throw new IllegalStateException(
                  "Shape "
                      + shape
                      + " not supported for "
                      + MemorySize.class.getName()
                      + " serialization");
          }
        }
      }

      return null;
    }
  }
}
