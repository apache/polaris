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
package org.apache.polaris.service.lineage.api;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;

/**
 * Resolves an OpenLineage event JSON body to one of {@link PolarisLineageEvent.OfRunEvent}, {@link
 * PolarisLineageEvent.OfJobEvent}, or {@link PolarisLineageEvent.OfDatasetEvent} by inspecting the
 * trailing path segment of the {@code schemaURL} field.
 *
 * <p>The OpenLineage spec requires every event to include a {@code schemaURL} pointing at the
 * variant's JSON schema fragment, e.g. {@code
 * https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent}. The fragment name is the
 * stable discriminator across spec versions.
 *
 * <p>If {@code schemaURL} is missing or unrecognized, the body is parsed as a {@code RunEvent} —
 * the most common variant and the same fallback Marquez (the OpenLineage reference server) uses.
 */
public class LineageEventTypeResolver extends TypeIdResolverBase {

  private JavaType superType;

  @Override
  public void init(JavaType baseType) {
    this.superType = baseType;
  }

  @Override
  public String idFromValue(Object value) {
    return idFromValueAndType(value, value == null ? null : value.getClass());
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType) {
    // Polaris does not currently emit lineage events; serialization is not on the
    // ingest path. Returning a stable label keeps Jackson happy if a downstream
    // module ever serializes a wrapper.
    if (suggestedType == PolarisLineageEvent.OfJobEvent.class) {
      return "JobEvent";
    }
    if (suggestedType == PolarisLineageEvent.OfDatasetEvent.class) {
      return "DatasetEvent";
    }
    return "RunEvent";
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) {
    Class<?> subType = subTypeFor(id);
    return context.constructSpecializedType(superType, subType);
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.CUSTOM;
  }

  private static Class<?> subTypeFor(String schemaUrl) {
    if (schemaUrl == null) {
      return PolarisLineageEvent.OfRunEvent.class;
    }
    String[] segments = schemaUrl.split("/");
    String tail = segments[segments.length - 1];
    return switch (tail) {
      case "JobEvent" -> PolarisLineageEvent.OfJobEvent.class;
      case "DatasetEvent" -> PolarisLineageEvent.OfDatasetEvent.class;
      default -> PolarisLineageEvent.OfRunEvent.class;
    };
  }
}
