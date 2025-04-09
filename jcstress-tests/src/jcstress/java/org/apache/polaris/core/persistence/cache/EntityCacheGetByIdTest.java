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
package org.apache.polaris.core.persistence.cache;

import static org.apache.polaris.core.entity.PolarisEntityType.CATALOG;
import static org.openjdk.jcstress.annotations.Expect.*;

import org.apache.polaris.core.PolarisCallContext;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;
import org.openjdk.jcstress.infra.results.I_Result;

public class EntityCacheGetByIdTest {

  @JCStressTest
  @Description(
      "Tests getOrLoadById is thread-safe.  In this test, two actors are calling getOrLoadById "
          + "twice on the same key.  Each actor returns the version of the entity returned by the "
          + "two calls to getOrLoadById.  Expected behaviour is that the two actors get the same "
          + "object twice, or for an object to be updated to a newer version between reads.  "
          + "But the cache should never go backward and serve a stale version after a newer one "
          + "has been observed as it is not allowed by `cacheNewEntry`.")
  @Outcome.Outcomes({
    @Outcome(id = "1, 1", expect = ACCEPTABLE, desc = "Got the same object twice"),
    @Outcome(id = "2, 2", expect = ACCEPTABLE, desc = "Got the same object twice"),
    @Outcome(id = "1, 2", expect = ACCEPTABLE_INTERESTING, desc = "Got updated object"),
    @Outcome(id = "2, 1", expect = FORBIDDEN, desc = "Got stale object after update"),
    @Outcome(expect = UNKNOWN, desc = "Not sure what happened"),
  })
  @State()
  public static class WithoutArbiter {
    private final PolarisCallContext context;
    private final EntityCache entityCache;

    public WithoutArbiter() {
      context = new PolarisCallContext(new FakeBasePersistence(), new FakePolarisDiagnostics());
      entityCache = new EntityCache(new FakeMetaStoreManager());
    }

    @Actor
    public void actor1(II_Result result) {
      result.r1 =
          entityCache
              .getOrLoadEntityById(context, 0L, FakeMetaStoreManager.CATALOG_ID, CATALOG)
              .getCacheEntry()
              .getEntity()
              .getEntityVersion();
      result.r2 =
          entityCache
              .getOrLoadEntityById(context, 0L, FakeMetaStoreManager.CATALOG_ID, CATALOG)
              .getCacheEntry()
              .getEntity()
              .getEntityVersion();
    }

    @Actor
    public void actor2(II_Result result) {
      result.r1 =
          entityCache
              .getOrLoadEntityById(context, 0L, FakeMetaStoreManager.CATALOG_ID, CATALOG)
              .getCacheEntry()
              .getEntity()
              .getEntityVersion();
      result.r2 =
          entityCache
              .getOrLoadEntityById(context, 0L, FakeMetaStoreManager.CATALOG_ID, CATALOG)
              .getCacheEntry()
              .getEntity()
              .getEntityVersion();
    }
  }

  @JCStressTest
  @Description(
      "Tests getOrLoadById is thread-safe.  In this test, two actors are calling getOrLoadById "
          + "twice on the same key.  The updates received by the actors are not checked as part of "
          + "this test.  Instead, an arbiter runs after the actors have performed their calls and "
          + "checks the version of the entity that is in the cache.  Expected behaviour is that "
          + "at most two database calls happens.  Thus only versions 1 and 2 are acceptable.  Any "
          + "other version indicates that the cache was updated even after both threads populated "
          + "it with their own values.")
  @Outcome.Outcomes({
    @Outcome(id = "1", expect = ACCEPTABLE, desc = "All cache calls happened in sequence"),
    @Outcome(id = "2", expect = ACCEPTABLE_INTERESTING, desc = "Concurrent cache updates happened"),
    @Outcome(expect = FORBIDDEN, desc = "Threads did not read their own writes"),
  })
  @State()
  public static class WithArbiter {
    private final PolarisCallContext context;
    private final EntityCache entityCache;

    public WithArbiter() {
      context = new PolarisCallContext(new FakeBasePersistence(), new FakePolarisDiagnostics());
      entityCache = new EntityCache(new FakeMetaStoreManager());
    }

    @Actor
    public void actor1() {
      entityCache.getOrLoadEntityById(context, 0L, FakeMetaStoreManager.CATALOG_ID, CATALOG);
      entityCache.getOrLoadEntityById(context, 0L, FakeMetaStoreManager.CATALOG_ID, CATALOG);
    }

    @Actor
    public void actor2() {
      entityCache.getOrLoadEntityById(context, 0L, FakeMetaStoreManager.CATALOG_ID, CATALOG);
      entityCache.getOrLoadEntityById(context, 0L, FakeMetaStoreManager.CATALOG_ID, CATALOG);
    }

    @Arbiter
    public void arbiter(I_Result result) {
      result.r1 =
          entityCache
              .getOrLoadEntityById(context, 0L, FakeMetaStoreManager.CATALOG_ID, CATALOG)
              .getCacheEntry()
              .getEntity()
              .getEntityVersion();
    }
  }
}
