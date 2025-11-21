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
package org.apache.polaris.persistence.nosql.api.obj;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public final class ObjTypes {
  private ObjTypes() {}

  @Nonnull
  public static ObjType objTypeById(@Nonnull String id) {
    var type = Registry.BY_ID.get(id);
    if (type == null) {
      type = Registry.genericType(id);
    }
    return type;
  }

  public static Map<String, ObjType> nonGenericObjTypes() {
    return Registry.BY_ID;
  }

  private static final class Registry {
    private static final Map<String, ObjType> BY_ID;
    private static final Map<String, ObjType> GENERIC_TYPES = new ConcurrentHashMap<>();

    static ObjType genericType(String name) {
      return GENERIC_TYPES.computeIfAbsent(name, GenObjType::new);
    }

    static final class GenObjType extends AbstractObjType<GenericObj> {
      GenObjType(String id) {
        super(id, "Generic (" + id + ")", GenericObj.class);
      }

      @Override
      public String name() {
        return "Generic ObjType (dynamically created)";
      }
    }

    static {
      var byId = new HashMap<String, ObjType>();
      var loader = ServiceLoader.load(ObjType.class);
      loader.stream()
          .map(ServiceLoader.Provider::get)
          .forEach(
              objType -> {
                ObjType ex = byId.put(objType.id(), objType);
                if (ex != null) {
                  throw new IllegalStateException(
                      format("Duplicate object type ID: from %s and %s", objType, ex));
                }
              });
      BY_ID = unmodifiableMap(byId);
    }
  }
}
