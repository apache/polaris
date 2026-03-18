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
package org.apache.polaris.persistence.nosql.impl.indexes;

import static com.google.common.base.Preconditions.checkNotNull;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;

/**
 * Internal index element representation.
 *
 * <p>This interface intentionally <em>breaks</em> the contract of {@link Index.Element} by allowing
 * {@code null} values, within the indexes implementation code. This is for space and performance,
 * aka to use the same object instance in the indexes API and the implementation.
 *
 * <p>Within a {@linkplain AbstractLayeredIndexImpl} index, the embedded reference index contains
 * {@code null} values for keys that have been removed but are still present in the spilled out
 * index segments.
 *
 * <p>The only place where instances of this type can "escape" is via the {@link Iterator} returning
 * functions of the {@link Index} interface. The implemntations of the {@code iterator()} and {@code
 * reverseIterator()} functions in {@link IndexSpi} uses {@link IndexElementIter} to ensure that
 * only elements with non-{@code null} values "escape".
 *
 * @param <V> element value type
 */
interface InternalIndexElement<V> extends Index.Element<V> {
  void serializeContent(IndexValueSerializer<V> ser, ByteBuffer target);

  int contentSerializedSize(IndexValueSerializer<V> ser);

  @Nullable
  V valueNullable();

  @Override
  @Nonnull
  default V value() {
    return checkNotNull(valueNullable(), key());
  }
}
