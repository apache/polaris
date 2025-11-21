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
package org.apache.polaris.persistence.nosql.api.index;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.immutables.value.Value;

/**
 * Describes a spilled-out index stripe, <em>do not use/interpret this type</em>.
 *
 * <p>This type is only used internally by code that manages index serialization and must never be
 * interpreted or even updated by any client code.
 *
 * <p>First/last key information is included to enable lazy-loading of required index stripes.
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableIndexStripe.class)
@JsonDeserialize(as = ImmutableIndexStripe.class)
public interface IndexStripe {
  @Value.Parameter
  IndexKey firstKey();

  @Value.Parameter
  IndexKey lastKey();

  @Value.Parameter
  ObjRef segment();

  static IndexStripe indexStripe(IndexKey firstKey, IndexKey lastKey, ObjRef segment) {
    return ImmutableIndexStripe.of(firstKey, lastKey, segment);
  }
}
