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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.immutables.value.Value;
import tools.jackson.databind.annotation.JsonTypeIdResolver;

@JsonTypeIdResolver(ChangeTypeIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "type", visible = true)
public interface Change {
  IndexValueSerializer<Change> CHANGE_SERIALIZER = new ChangeSerializer();

  @Value.Redacted
  @JsonIgnore
  // must use 'get*' here, otherwise the property won't be properly "wired" to be the type info
  ChangeType getType();
}
