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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Locale;
import tools.jackson.databind.DatabindContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.jsontype.impl.TypeIdResolverBase;

final class ChangeTypeIdResolver extends TypeIdResolverBase {

  private JavaType baseType;

  public ChangeTypeIdResolver() {}

  @Override
  public void init(JavaType bt) {
    baseType = bt;
  }

  @Override
  public String idFromValue(DatabindContext databindContext, Object value) {
    return getId(value);
  }

  @Override
  public String idFromValueAndType(
      DatabindContext databindContext, Object value, Class<?> suggestedType) {
    return getId(value);
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.CUSTOM;
  }

  private String getId(Object value) {
    if (value instanceof Change change) {
      return change.getType().name();
    }

    return null;
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) {
    var idLower = id.toLowerCase(Locale.ROOT);
    var asType = ChangeType.TYPE_MAP.get(idLower);
    if (asType == null) {
      return context.constructSpecializedType(baseType, GenericChange.class);
    }
    if (baseType.getRawClass().isAssignableFrom(asType)) {
      return context.constructSpecializedType(baseType, asType);
    }

    // This is rather a "test-only" code path, but it might happen in real life as well, when
    // calling the ObjectMapper with a "too specific" type and not just Change.class.
    // So we can get here, for example, if the baseType (induced by the type passed to
    // ObjectMapper), is GenericChange.class, but the type is a "well known" type like
    // ChangeRename.class.
    @SuppressWarnings("unchecked")
    var concrete = (Class<? extends Change>) baseType.getRawClass();
    return context.constructSpecializedType(baseType, concrete);
  }
}
