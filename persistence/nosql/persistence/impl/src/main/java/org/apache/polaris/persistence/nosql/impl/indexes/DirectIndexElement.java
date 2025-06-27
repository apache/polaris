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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.index.IndexValueSerializer;

final class DirectIndexElement<V> extends AbstractIndexElement<V> {
  private final IndexKey key;
  private final V content;

  DirectIndexElement(@Nonnull IndexKey key, @Nullable V content) {
    this.key = key;
    this.content = content;
  }

  @Override
  public IndexKey getKey() {
    return key;
  }

  @Override
  public V getValue() {
    return content;
  }

  @Override
  public V setValue(V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void serializeContent(IndexValueSerializer<V> ser, ByteBuffer target) {
    ser.serialize(content, target);
  }

  @Override
  public int contentSerializedSize(IndexValueSerializer<V> ser) {
    return ser.serializedSize(content);
  }
}
