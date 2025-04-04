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
package org.apache.polaris.persistence.coretypes.content;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import org.apache.polaris.persistence.api.index.IndexValueSerializer;

/** The "no value" type for {@link org.apache.polaris.persistence.api.index.Index} values. */
public final class Empty {

  private Empty() {}

  @SuppressWarnings("InstantiationOfUtilityClass")
  public static final Empty EMPTY = new Empty();

  public static final IndexValueSerializer<Empty> EMPTY_SERIALIZER =
      new IndexValueSerializer<>() {
        @Override
        public int serializedSize(@Nullable Empty value) {
          return 0;
        }

        @Override
        @Nonnull
        public ByteBuffer serialize(@Nullable Empty value, @Nonnull ByteBuffer target) {
          return target;
        }

        @Override
        public Empty deserialize(@Nonnull ByteBuffer buffer) {
          return Empty.EMPTY;
        }

        @Override
        public void skip(@Nonnull ByteBuffer buffer) {}
      };
}
