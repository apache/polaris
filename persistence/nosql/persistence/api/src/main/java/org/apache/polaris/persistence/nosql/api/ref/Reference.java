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
package org.apache.polaris.persistence.nosql.api.ref;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.obj.Obj;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

@PolarisImmutable
@JsonSerialize(as = ImmutableReference.class)
@JsonDeserialize(as = ImmutableReference.class)
public interface Reference {
  String name();

  /**
   * Current pointer of this reference.
   *
   * <p>Note that the pointer can only be {@linkplain Optional#empty()} when a reference has been
   * created without a current pointer. If a reference ever had a pointer value, it cannot be
   * "reset" to become {@linkplain Optional#empty()} again.
   *
   * @return current pointer, {@linkplain Optional#empty()} for references that have been created
   *     without a current pointer.
   */
  Optional<ObjRef> pointer();

  /**
   * Timestamp in microseconds since (Unix) epoch when the reference was created in the database.
   */
  long createdAtMicros();

  /**
   * List of previously assigned {@linkplain #pointer() pointers}.
   *
   * <p>This list can be useful in case of disaster recovery scenarios in combination with
   * geographically distributed databases / replication, when in the disaster recovery case the
   * whole database content is not consistently available.
   *
   * <p>The "full" {@link ObjRef}s can be re-constructed by using the {@link #pointer()
   * pointer()}{@code .}{@link Optional#get() get()}{@code .}{@link Obj#type()}.
   */
  long[] previousPointers();

  static ImmutableReference.Builder builder() {
    return ImmutableReference.builder();
  }
}
