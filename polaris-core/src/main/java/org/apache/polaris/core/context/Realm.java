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
package org.apache.polaris.core.context;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Represents the ID of the realm used in a REST request associated with routing to independent and
 * isolated "universes".
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableRealm.class)
@JsonDeserialize(as = ImmutableRealm.class)
public interface Realm {

  static Realm newRealm(String id) {
    return ImmutableRealm.of(id);
  }

  static Realm copyOf(Realm realm) {
    return ImmutableRealm.copyOf(realm);
  }

  @Value.Parameter
  @JsonValue
  String id();
}
