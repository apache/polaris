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
package org.apache.polaris.persistence.nosql.impl.cache;

import java.time.Duration;
import java.time.Instant;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.module.SimpleModule;

/** Cache-only numeric pairs preserve nanosecond precision and avoid decimal conversion. */
final class CacheTimeModule extends SimpleModule {
  CacheTimeModule() {
    super("cache-time");
    addSerializer(
        Instant.class,
        new ValueSerializer<>() {
          @Override
          public void serialize(Instant value, JsonGenerator gen, SerializationContext context) {
            gen.writeStartArray();
            gen.writeNumber(value.getEpochSecond());
            gen.writeNumber(value.getNano());
            gen.writeEndArray();
          }
        });
    addDeserializer(
        Instant.class,
        new ValueDeserializer<>() {
          @Override
          public Instant deserialize(JsonParser parser, DeserializationContext context) {
            if (!parser.isExpectedStartArrayToken()) {
              return (Instant) context.handleUnexpectedToken(Instant.class, parser);
            }
            parser.nextToken();
            var seconds = parser.getLongValue();
            parser.nextToken();
            var nanos = parser.getIntValue();
            if (parser.nextToken() != JsonToken.END_ARRAY) {
              return context.reportInputMismatch(Instant.class, "Expected [seconds, nanos]");
            }
            return Instant.ofEpochSecond(seconds, nanos);
          }
        });
    addSerializer(
        Duration.class,
        new ValueSerializer<>() {
          @Override
          public void serialize(Duration value, JsonGenerator gen, SerializationContext context) {
            gen.writeStartArray();
            gen.writeNumber(value.getSeconds());
            gen.writeNumber(value.getNano());
            gen.writeEndArray();
          }
        });
    addDeserializer(
        Duration.class,
        new ValueDeserializer<>() {
          @Override
          public Duration deserialize(JsonParser parser, DeserializationContext context) {
            if (!parser.isExpectedStartArrayToken()) {
              return (Duration) context.handleUnexpectedToken(Duration.class, parser);
            }
            parser.nextToken();
            var seconds = parser.getLongValue();
            parser.nextToken();
            var nanos = parser.getIntValue();
            if (parser.nextToken() != JsonToken.END_ARRAY) {
              return context.reportInputMismatch(Duration.class, "Expected [seconds, nanos]");
            }
            return Duration.ofSeconds(seconds, nanos);
          }
        });
  }
}
