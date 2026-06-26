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

import static org.apache.polaris.persistence.nosql.api.index.IndexKey.key;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.OBJ_REF_SERIALIZER;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.deserializeStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.indexElement;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.lazyStoreIndex;
import static org.apache.polaris.persistence.nosql.impl.indexes.IndexesInternal.newStoreIndex;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.index.Index;
import org.apache.polaris.persistence.nosql.api.index.IndexKey;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;

final class LazyIndexImplJCStressSupport {
  static final IndexKey FIRST_KEY = key("aaa");
  static final IndexKey LAST_KEY = key("zzz");
  static final ObjRef OBJ_REF = objRef("stress", 7L, 1);
  static final ObjRef ASSIGNED_REF = objRef("stress", 8L, 2);

  private LazyIndexImplJCStressSupport() {}

  static IndexSpi<ObjRef> baseIndex() {
    var base = newStoreIndex(OBJ_REF_SERIALIZER);
    base.add(indexElement(FIRST_KEY, OBJ_REF));
    base.add(indexElement(LAST_KEY, OBJ_REF));
    return deserializeStoreIndex(base.serialize(), OBJ_REF_SERIALIZER);
  }

  static IndexSpi<ObjRef> lazyIndex(Supplier<IndexSpi<ObjRef>> supplier) {
    return lazyStoreIndex(supplier, FIRST_KEY, LAST_KEY);
  }

  static int count(Index<ObjRef> index) {
    var count = 0;
    for (Index.Element<ObjRef> ignored : index) {
      count++;
    }
    return count;
  }

  static void await(CountDownLatch latch) {
    try {
      if (!latch.await(1, TimeUnit.SECONDS)) {
        throw new AssertionError("Timed out waiting for latch");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  static final class CountingSupplier implements Supplier<IndexSpi<ObjRef>> {
    final AtomicInteger calls = new AtomicInteger();
    final IndexSpi<ObjRef> delegate = baseIndex();

    @Override
    public IndexSpi<ObjRef> get() {
      calls.incrementAndGet();
      return delegate;
    }
  }

  static final class FailingSupplier implements Supplier<IndexSpi<ObjRef>> {
    final AtomicInteger calls = new AtomicInteger();
    final RuntimeException failure = new RuntimeException("jcstress failure");

    @Override
    public IndexSpi<ObjRef> get() {
      calls.incrementAndGet();
      throw failure;
    }
  }

  static final class BlockingSupplier implements Supplier<IndexSpi<ObjRef>> {
    final AtomicInteger calls = new AtomicInteger();
    final IndexSpi<ObjRef> delegate = baseIndex();
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch continueLoad = new CountDownLatch(1);

    @Override
    public IndexSpi<ObjRef> get() {
      calls.incrementAndGet();
      started.countDown();
      await(continueLoad);
      return delegate;
    }
  }
}
