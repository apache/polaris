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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;

public interface IndexValueSerializer<V> {
  int serializedSize(@Nullable V value);

  /** Serialize {@code value} into {@code target}, returns {@code target}. */
  @Nonnull
  ByteBuffer serialize(@Nullable V value, @Nonnull ByteBuffer target);

  /**
   * Deserialize a value from {@code buffer}. Implementations must not assume that the given {@link
   * ByteBuffer} only contains data for the value to deserialize, other data likely follows.
   */
  @Nullable
  V deserialize(@Nonnull ByteBuffer buffer);

  /** Skips an element, only updating the {@code buffer}'s {@link ByteBuffer#position()}. */
  void skip(@Nonnull ByteBuffer buffer);
}
