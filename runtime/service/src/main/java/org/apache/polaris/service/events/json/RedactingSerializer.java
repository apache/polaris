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
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

/**
 * A Jackson serializer that redacts string values by replacing them with a redaction marker.
 *
 * <p>This serializer is used to prevent sensitive information from being included in serialized
 * JSON output, particularly for event logging to external systems like CloudWatch.
 */
public class RedactingSerializer extends JsonSerializer<Object> {
  private static final String REDACTED_MARKER = "***REDACTED***";

  @Override
  public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeString(REDACTED_MARKER);
  }

  /**
   * Returns the marker string used to indicate redacted values.
   *
   * @return the redaction marker string
   */
  public static String getRedactedMarker() {
    return REDACTED_MARKER;
  }
}

