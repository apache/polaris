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

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

@PolarisImmutable
@JsonDeserialize(as = ImmutableChangeType.class)
@JsonSerialize(as = ImmutableChangeType.class)
public interface ChangeType {
  ChangeType ADD = valueOf("add");
  ChangeType UPDATE = valueOf("update");
  ChangeType REMOVE = valueOf("remove");
  ChangeType RENAME = valueOf("rename");

  Map<String, Class<? extends Change>> TYPE_MAP =
      Map.of(
          ADD.name(),
          ChangeAdd.class,
          UPDATE.name(),
          ChangeUpdate.class,
          REMOVE.name(),
          ChangeRemove.class,
          RENAME.name(),
          ChangeRename.class);

  @JsonValue
  @Value.Parameter
  String name();

  static ChangeType valueOf(String name) {
    return ImmutableChangeType.of(name);
  }
}
