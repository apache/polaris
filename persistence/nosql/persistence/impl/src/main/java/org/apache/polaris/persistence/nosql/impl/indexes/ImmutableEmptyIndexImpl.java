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

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;
import org.apache.polaris.persistence.varint.VarInt;

final class ImmutableEmptyIndexImpl<V> implements IndexSpi<V> {

  private final IndexValueSerializer<V> serializer;

  ImmutableEmptyIndexImpl(IndexValueSerializer<V> serializer) {
    this.serializer = serializer;
  }

  @Override
  public boolean hasElements() {
    return false;
  }

  @Override
  public boolean isModified() {
    return false;
  }

  @Override
  public void prefetchIfNecessary(Iterable<IndexKey> keys) {}

  @Override
  public boolean isLoaded() {
    return true;
  }

  @Override
  public IndexSpi<V> asMutableIndex() {
    return newStoreIndex(serializer);
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public List<IndexSpi<V>> divide(int parts) {
    throw unsupported();
  }

  @Override
  public List<IndexSpi<V>> stripes() {
    return emptyList();
  }

  @Override
  public IndexSpi<V> mutableStripeForKey(IndexKey key) {
    throw unsupported();
  }

  @Override
  public boolean add(@Nonnull IndexElement<V> element) {
    throw unsupported();
  }

  @Override
  public boolean remove(@Nonnull IndexKey key) {
    throw unsupported();
  }

  @Override
  public boolean contains(@Nonnull IndexKey key) {
    return false;
  }

  @Override
  public boolean containsElement(@Nonnull IndexKey key) {
    return false;
  }

  @Nullable
  @Override
  public IndexElement<V> getElement(@Nonnull IndexKey key) {
    return null;
  }

  @Nullable
  @Override
  public IndexKey first() {
    return null;
  }

  @Nullable
  @Override
  public IndexKey last() {
    return null;
  }

  @Override
  public List<IndexKey> asKeyList() {
    return emptyList();
  }

  @Nonnull
  @Override
  public Iterator<IndexElement<V>> elementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return emptyIterator();
  }

  @Override
  public Iterator<IndexElement<V>> reverseElementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return emptyIterator();
  }

  @Override
  public int estimatedSerializedSize() {
    return 2; // index-version byte + VarInt.varIntLen(0) --> 1+1
  }

  @Nonnull
  @Override
  public ByteBuffer serialize() {
    var target = ByteBuffer.allocate(estimatedSerializedSize());

    // Serialized segment index version
    target.put((byte) 1);

    VarInt.putVarInt(target, 0);

    target.flip();
    return target;
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Operation not supported for non-mutable indexes");
  }
}
