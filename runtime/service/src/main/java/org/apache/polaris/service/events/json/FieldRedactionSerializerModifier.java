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

package org.apache.polaris.service.events.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A Jackson BeanSerializerModifier that redacts fields based on configured patterns.
 *
 * <p>This modifier intercepts serialization of bean properties and replaces values with a redaction
 * marker for fields that match the configured patterns. It supports:
 *
 * <ul>
 *   <li>Simple field names (e.g., "password", "secret")
 *   <li>Nested field paths using dot notation (e.g., "properties.api-key")
 *   <li>Wildcard patterns (e.g., "*.secret", "credentials.*")
 * </ul>
 */
public class FieldRedactionSerializerModifier extends BeanSerializerModifier {

  private final List<Pattern> fieldPatterns;

  /**
   * Creates a new FieldRedactionSerializerModifier with the specified field patterns.
   *
   * @param fieldNames set of field names or patterns to redact
   */
  public FieldRedactionSerializerModifier(Set<String> fieldNames) {
    this.fieldPatterns =
        fieldNames.stream()
            .map(FieldRedactionSerializerModifier::convertToRegexPattern)
            .map(Pattern::compile)
            .collect(Collectors.toList());
  }

  /**
   * Converts a field pattern (which may contain wildcards) to a regex pattern.
   *
   * @param pattern the field pattern (e.g., "*.secret", "credentials.*", "exact-field")
   * @return a regex pattern string
   */
  private static String convertToRegexPattern(String pattern) {
    // Escape special regex characters except for our wildcard (*)
    String escaped = pattern.replace(".", "\\.").replace("*", ".*");
    // Match the entire field path
    return "^" + escaped + "$";
  }

  @Override
  public List<BeanPropertyWriter> changeProperties(
      SerializationConfig config,
      BeanDescription beanDesc,
      List<BeanPropertyWriter> beanProperties) {
    
    if (fieldPatterns.isEmpty()) {
      return beanProperties;
    }

    return beanProperties.stream()
        .map(writer -> shouldRedactField(writer.getName()) ? createRedactingWriter(writer) : writer)
        .collect(Collectors.toList());
  }

  /**
   * Checks if a field name matches any of the configured redaction patterns.
   *
   * @param fieldName the field name to check
   * @return true if the field should be redacted
   */
  private boolean shouldRedactField(String fieldName) {
    return fieldPatterns.stream().anyMatch(pattern -> pattern.matcher(fieldName).matches());
  }

  /**
   * Creates a new BeanPropertyWriter that redacts the field value.
   *
   * @param original the original BeanPropertyWriter
   * @return a new BeanPropertyWriter that redacts the value
   */
  private BeanPropertyWriter createRedactingWriter(BeanPropertyWriter original) {
    return new BeanPropertyWriter(original) {
      @Override
      public void serializeAsField(Object bean, JsonGenerator gen, SerializerProvider prov)
          throws Exception {
        gen.writeFieldName(_name);
        gen.writeString(RedactingSerializer.getRedactedMarker());
      }
    };
  }
}

