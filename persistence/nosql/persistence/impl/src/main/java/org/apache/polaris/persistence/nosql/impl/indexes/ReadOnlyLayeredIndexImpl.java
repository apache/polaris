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

import static java.util.Collections.singletonList;

import jakarta.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;

/**
 * Combines two {@link Index store indexes}, where one index serves as the "reference" and the other
 * containing "updates".
 *
 * <p>A layered index contains all keys from both indexes. The value of a key that is present in
 * both indexes will be provided from the "embedded" index.
 */
final class ReadOnlyLayeredIndexImpl<V> extends AbstractLayeredIndexImpl<V> {

  ReadOnlyLayeredIndexImpl(IndexSpi<V> reference, IndexSpi<V> embedded) {
    super(reference, embedded);
  }

  @Override
  public IndexSpi<V> asMutableIndex() {
    throw unsupported();
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
    return singletonList(this);
  }

  @Override
  public IndexSpi<V> mutableStripeForKey(IndexKey key) {
    throw unsupported();
  }

  @Nonnull
  @Override
  public ByteBuffer serialize() {
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

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Layered indexes do not support this operation");
  }
}
