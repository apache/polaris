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
package org.apache.polaris.persistence.nosql.inmemory;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.backend.PersistId;
import org.immutables.value.Value;

@PolarisImmutable
@JsonSerialize(as = ImmutableObjKey.class)
@JsonDeserialize(as = ImmutableObjKey.class)
public interface ObjKey {
  static ObjKey objKey(String realmId, long id, int part) {
    return ImmutableObjKey.of(realmId, id, part);
  }

  static ObjKey objKey(String realmId, PersistId persistId) {
    return ImmutableObjKey.of(realmId, persistId.id(), persistId.part());
  }

  @Value.Parameter(order = 1)
  String realmId();

  @Value.Parameter(order = 2)
  long id();

  @Value.Parameter(order = 3)
  int part();
}
