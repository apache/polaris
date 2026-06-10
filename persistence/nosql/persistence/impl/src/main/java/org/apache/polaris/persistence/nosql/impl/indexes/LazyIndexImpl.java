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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

final class LazyIndexImpl<V> implements IndexSpi<V> {
  private static final Object STATE_UNLOADED = new Object();
  private static final Object STATE_LOADING = new Object();

  private final Supplier<IndexSpi<V>> loader;
  private volatile Object loadState = STATE_UNLOADED;
  private volatile ObjRef objRef;
  private final IndexKey firstKey;
  private final IndexKey lastKey;
  private Thread loadingThread;

  LazyIndexImpl(Supplier<IndexSpi<V>> supplier, IndexKey firstKey, IndexKey lastKey) {
    this.firstKey = firstKey;
    this.lastKey = lastKey;
    this.loader = supplier;
  }

  private IndexSpi<V> loaded() {
    var state = loadState;
    if (notLoadedYet(state)) {
      return loadOrAwait();
    }
    return resolvedState(state);
  }

  private static boolean notLoadedYet(Object state) {
    return state == STATE_UNLOADED || state == STATE_LOADING;
  }

  @SuppressWarnings("unchecked")
  private static <V> IndexSpi<V> resolvedState(Object state) {
    if (state instanceof RuntimeException re) {
      throw re;
    }
    if (state instanceof Error err) {
      throw err;
    }
    return (IndexSpi<V>) state;
  }

  private IndexSpi<V> loadOrAwait() {
    Thread current = Thread.currentThread();
    synchronized (this) {
      var state = loadState;
      if (state == STATE_UNLOADED) {
        loadState = STATE_LOADING;
        loadingThread = current;
      } else if (state == STATE_LOADING) {
        if (loadingThread == current) {
          throw new IllegalStateException("Recursive call to LazyIndexImpl.loaded() detected");
        }
        var interrupted = false;
        try {
          while ((state = loadState) == STATE_LOADING) {
            try {
              wait();
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        } finally {
          if (interrupted) {
            current.interrupt();
          }
        }
        return resolvedState(state);
      } else {
        return resolvedState(state);
      }
    }

    try {
      var loaded = loader.get();
      completeLoad(loaded);
      return loaded;
    } catch (RuntimeException | Error e) {
      completeLoad(e);
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  private void completeLoad(Object state) {
    synchronized (this) {
      if (state instanceof IndexSpi<?> index) {
        var objRef = this.objRef;
        if (objRef != null) {
          ((IndexSpi<V>) index).setObjId(objRef);
        }
      }
      loadState = state;
      loadingThread = null;
      notifyAll();
    }
  }

  @Override
  public ObjRef getObjId() {
    return objRef;
  }

  @Override
  public IndexSpi<V> setObjId(ObjRef objRef) {
    synchronized (this) {
      this.objRef = objRef;
      var state = loadState;
      if (!notLoadedYet(state)) {
        resolvedState(state).setObjId(objRef);
      }
    }
    return this;
  }

  @Override
  public boolean isModified() {
    if (notLoadedYet(loadState)) {
      return false;
    }
    return loaded().isModified();
  }

  @Override
  public void prefetchIfNecessary(Iterable<IndexKey> keys) {
    loaded().prefetchIfNecessary(keys);
  }

  @Override
  public boolean isLoaded() {
    return !notLoadedYet(loadState);
  }

  @Override
  public IndexSpi<V> asMutableIndex() {
    return loaded().asMutableIndex();
  }

  @Override
  public boolean isMutable() {
    if (notLoadedYet(loadState)) {
      return false;
    }
    return loaded().isMutable();
  }

  @Override
  public List<IndexSpi<V>> splitByTargetSize(long targetSerializedSize) {
    return loaded().splitByTargetSize(targetSerializedSize);
  }

  @Override
  public List<IndexSpi<V>> stripes() {
    return loaded().stripes();
  }

  @Override
  public IndexSpi<V> mutableStripeForKey(IndexKey key) {
    return loaded().mutableStripeForKey(key);
  }

  @Override
  public boolean hasElements() {
    return loaded().hasElements();
  }

  @Override
  public int estimatedSerializedSize() {
    return loaded().estimatedSerializedSize();
  }

  @Override
  public boolean add(@NonNull InternalIndexElement<V> element) {
    return loaded().add(element);
  }

  @Override
  public boolean remove(@NonNull IndexKey key) {
    return loaded().remove(key);
  }

  @Override
  public boolean contains(@NonNull IndexKey key) {
    if (notLoadedYet(loadState) && (key.equals(firstKey) || key.equals(lastKey))) {
      return true;
    }
    return loaded().contains(key);
  }

  @Override
  public boolean containsElement(@NonNull IndexKey key) {
    if (notLoadedYet(loadState) && (key.equals(firstKey) || key.equals(lastKey))) {
      return true;
    }
    return loaded().containsElement(key);
  }

  @Override
  @Nullable
  public InternalIndexElement<V> getElement(@NonNull IndexKey key) {
    return loaded().getElement(key);
  }

  @Override
  @Nullable
  public IndexKey first() {
    if (notLoadedYet(loadState) && firstKey != null) {
      return firstKey;
    }
    return loaded().first();
  }

  @Override
  @Nullable
  public IndexKey last() {
    if (notLoadedYet(loadState) && lastKey != null) {
      return lastKey;
    }
    return loaded().last();
  }

  @Override
  public List<IndexKey> asKeyList() {
    return loaded().asKeyList();
  }

  @Override
  @NonNull
  public Iterator<InternalIndexElement<V>> elementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return loaded().elementIterator(lower, higher, prefetch);
  }

  @Override
  @NonNull
  public Iterator<InternalIndexElement<V>> reverseElementIterator(
      @Nullable IndexKey lower, @Nullable IndexKey higher, boolean prefetch) {
    return loaded().reverseElementIterator(lower, higher, prefetch);
  }

  @Override
  @NonNull
  public ByteBuffer serialize() {
    return loaded().serialize();
  }
}
