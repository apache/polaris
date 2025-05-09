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
import static org.apache.polaris.core.persistence.cache.FakeMetaStoreManager.CATALOG_ID;
import static org.openjdk.jcstress.annotations.Expect.*;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.LL_Result;

public class EntityCacheCoherenceTest {
  /** The cache key associated with the catalog that is returned by the fake meta store manager. */
  private static final EntityCacheByNameKey KEY = new EntityCacheByNameKey(CATALOG, "test");

  @JCStressTest
  @Description(
      "Tests that the two access methods inside EntityCache are coherent with one another.  In "
          + "this test, two actors are calling getOrLoadById and getOrLoadByName.  The entities "
          + "received by the actors are not checked as part of this test.  Instead, an arbiter "
          + "runs after the actors have performed their calls and checks the version of the "
          + "entity that are in each of the two caches using the getter methods getEntityById "
          + "and getEntityByName.  Expected behaviour is that the two caches return the same "
          + "entity version.  Any discrepancy indicates that the caches appear to be out of sync.")
  @Outcome.Outcomes({
    @Outcome(id = "1, 1", expect = ACCEPTABLE, desc = "The caches are in sync"),
    @Outcome(expect = FORBIDDEN, desc = "Not sure what happened"),
  })
  @State()
  public static class UsingGetters {
    private final PolarisCallContext context;
    private final InMemoryEntityCache entityCache;

    public UsingGetters() {
      context = new PolarisCallContext(new FakeBasePersistence(), new FakePolarisDiagnostics());
      entityCache = new InMemoryEntityCache(new FakeMetaStoreManager());
    }

    @Actor
    public void actor1() {
      entityCache
          .getOrLoadEntityById(context, 0L, CATALOG_ID, CATALOG)
          .getCacheEntry()
          .getEntity()
          .getEntityVersion();
      entityCache
          .getOrLoadEntityByName(context, KEY)
          .getCacheEntry()
          .getEntity()
          .getEntityVersion();
    }

    @Actor
    public void actor2() {
      entityCache
          .getOrLoadEntityById(context, 0L, CATALOG_ID, CATALOG)
          .getCacheEntry()
          .getEntity()
          .getEntityVersion();
      entityCache
          .getOrLoadEntityByName(context, KEY)
          .getCacheEntry()
          .getEntity()
          .getEntityVersion();
    }

    @Arbiter
    public void arbiter(LL_Result result) {
      result.r1 = extractEntityVersion(entityCache.getEntityById(CATALOG_ID));
      result.r2 = extractEntityVersion(entityCache.getEntityByName(KEY));
    }

    private static Integer extractEntityVersion(ResolvedPolarisEntity e) {
      if (e == null || e.getEntity() == null) {
        return null;
      }
      return e.getEntity().getEntityVersion();
    }
  }

  @JCStressTest
  @Description(
      "Tests that the two access methods inside EntityCache are coherent with one another.  In "
          + "this test, two actors are calling getOrLoadById and getOrLoadByName.  The entities "
          + "received by the actors are not checked as part of this test.  Instead, an arbiter "
          + "runs after the actors have performed their calls and checks the version of the "
          + "entity that are in each of the two caches using the getter methods getOrLoadById "
          + "and getOrLoadByName.  Expected behaviour is that the two caches return the same "
          + "entity version.  Any discrepancy indicates that the caches appear to be out of sync.")
  @Outcome.Outcomes({
    @Outcome(id = "1, 1", expect = ACCEPTABLE, desc = "The caches are in sync"),
    @Outcome(id = "1, 2", expect = FORBIDDEN, desc = "The caches are out of sync"),
    @Outcome(id = "2, 2", expect = ACCEPTABLE, desc = "The caches are in sync"),
    @Outcome(id = "2, 1", expect = FORBIDDEN, desc = "The caches are out of sync"),
    @Outcome(expect = UNKNOWN, desc = "Not sure what happened"),
  })
  @State()
  public static class UsingLoaders {
    private final PolarisCallContext context;
    private final EntityCache entityCache;

    public UsingLoaders() {
      context = new PolarisCallContext(new FakeBasePersistence(), new FakePolarisDiagnostics());
      entityCache = new InMemoryEntityCache(new FakeMetaStoreManager());
    }

    @Actor
    public void actor1() {
      entityCache
          .getOrLoadEntityById(context, 0L, CATALOG_ID, CATALOG)
          .getCacheEntry()
          .getEntity()
          .getEntityVersion();
      entityCache
          .getOrLoadEntityByName(context, KEY)
          .getCacheEntry()
          .getEntity()
          .getEntityVersion();
    }

    @Actor
    public void actor2() {
      entityCache
          .getOrLoadEntityById(context, 0L, CATALOG_ID, CATALOG)
          .getCacheEntry()
          .getEntity()
          .getEntityVersion();
      entityCache
          .getOrLoadEntityByName(context, KEY)
          .getCacheEntry()
          .getEntity()
          .getEntityVersion();
    }

    @Arbiter
    public void arbiter(LL_Result result) {
      result.r1 =
          extractEntityVersion(entityCache.getOrLoadEntityById(context, 0L, CATALOG_ID, CATALOG));
      result.r2 = extractEntityVersion(entityCache.getOrLoadEntityByName(context, KEY));
    }

    private static Integer extractEntityVersion(ResolvedPolarisEntity e) {
      if (e == null || e.getEntity() == null) {
        return null;
      }
      return e.getEntity().getEntityVersion();
    }

    private static Integer extractEntityVersion(EntityCacheLookupResult e) {
      if (e == null || e.getCacheEntry() == null || e.getCacheEntry().getEntity() == null) {
        return null;
      }
      return e.getCacheEntry().getEntity().getEntityVersion();
    }
  }
}
