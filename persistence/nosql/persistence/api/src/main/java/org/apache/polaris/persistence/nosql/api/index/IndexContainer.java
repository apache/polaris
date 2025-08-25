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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.polaris.immutables.PolarisImmutable;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.obj.Obj;

/**
 * Container to hold an index, to be used as an attribute in {@link Obj}s, see the rules below which
 * functions to use.
 *
 * <p>Do <em>not</em> access the {@link #embedded()} and {@link #stripes()} attributes directly, use
 * the {@link #indexForRead(Persistence, IndexValueSerializer) indexForRead()}/{@link
 * #asUpdatableIndex(Persistence, IndexValueSerializer) asUpdatableIndex()} functions instead.
 *
 * <p>Do <em>not</em> construct an {@link IndexContainer} directly, use the {@link
 * #newUpdatableIndex(Persistence, IndexValueSerializer) newUpdatableIndex()} and {@link
 * #asUpdatableIndex(Persistence, IndexValueSerializer) asUpdatableIndex()} functions.
 *
 * @param <V> value type
 */
@PolarisImmutable
@JsonSerialize(as = ImmutableIndexContainer.class)
@JsonDeserialize(as = ImmutableIndexContainer.class)
public interface IndexContainer<V> {

  /**
   * Returns a read-only representation of the whole index from index information in this container.
   * The returned index cannot be used for any serialization or any other write-intended operations.
   */
  default Index<V> indexForRead(
      @Nonnull Persistence persistence, @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    return persistence.buildReadIndex(this, indexValueSerializer);
  }

  /**
   * Builds an {@link UpdatableIndex} from index information in this container, to eventually
   * {@linkplain UpdatableIndex#toIndexed(String, BiConsumer) build a new index container}, using
   * the given element serializer.
   */
  default UpdatableIndex<V> asUpdatableIndex(
      @Nonnull Persistence persistence, @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    return persistence.buildWriteIndex(this, indexValueSerializer);
  }

  /**
   * Creates a new {@link UpdatableIndex} to eventually {@linkplain UpdatableIndex#toIndexed(String,
   * BiConsumer) build a new index container}, using the given element serializer.
   */
  static <V> UpdatableIndex<V> newUpdatableIndex(
      @Nonnull Persistence persistence, @Nonnull IndexValueSerializer<V> indexValueSerializer) {
    return persistence.buildWriteIndex(null, indexValueSerializer);
  }

  /** DO NOT ACCESS DIRECTLY, this is the serialized representation of the "embedded" index. */
  ByteBuffer embedded();

  /**
   * DO NOT ACCESS DIRECTLY, these are pointers to the composite reference index stripes, an
   * "embedded" version of {@code IndexSegmentsObj}. Index container objects that require to
   * "externalize" index elements to a reference index, which requires up to {@link
   * PersistenceParams#maxIndexStripes()} will be kept here and not create another indirection via a
   * {@code IndexSegmentsObj}.
   *
   * @see #embedded()
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<IndexStripe> stripes();
}
