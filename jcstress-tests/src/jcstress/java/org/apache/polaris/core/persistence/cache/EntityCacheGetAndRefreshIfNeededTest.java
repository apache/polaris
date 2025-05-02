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

import static org.openjdk.jcstress.annotations.Expect.*;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;

@JCStressTest
@Description(
    "Tests getAndRefreshIfNeeded is thread-safe.  In this test, two actors are calling"
        + " getAndRefreshIfNeeded twice on the same key.  Each actor returns the version of the"
        + " entity returned by the two calls to getAndRefreshIfNeeded.  The method is invoked using"
        + " a minimum entity version equal to 2.  Expected behaviour is that the two actors get"
        + " the same object twice.  But the cache should never go backward in time and serve a"
        + " stale version after a newer one has been observed.  It should also not refresh an"
        + " entity that is already at a sufficientlty high version id in cache.")
@Outcome.Outcomes({
  @Outcome(id = "2, 2", expect = ACCEPTABLE, desc = "Stable cache behaviour"),
  @Outcome(id = "2, 3", expect = FORBIDDEN, desc = "Concurrent cache update"),
  @Outcome(id = "3, 3", expect = FORBIDDEN, desc = "Race condition: no thread got v2"),
  @Outcome(id = "3, 2", expect = FORBIDDEN, desc = "Cache went back in time"),
  @Outcome(expect = UNKNOWN, desc = "Not sure what happened"),
})
@State()
public class EntityCacheGetAndRefreshIfNeededTest {

  private final PolarisCallContext context;
  private final EntityCache entityCache;
  private final PolarisBaseEntity entityV1;

  public EntityCacheGetAndRefreshIfNeededTest() {
    context = new PolarisCallContext(new FakeBasePersistence(), new FakePolarisDiagnostics());
    FakeMetaStoreManager metaStoreManager = new FakeMetaStoreManager();
    entityCache = new InMemoryEntityCache(metaStoreManager);
    entityV1 = metaStoreManager.loadResolvedEntityById(null, -1, -1, null).getEntity();
  }

  @Actor
  public void actor1(II_Result result) {
    result.r1 =
        entityCache.getAndRefreshIfNeeded(context, entityV1, 2, 0).getEntity().getEntityVersion();
    result.r2 =
        entityCache.getAndRefreshIfNeeded(context, entityV1, 2, 0).getEntity().getEntityVersion();
  }

  @Actor
  public void actor2(II_Result result) {
    result.r1 =
        entityCache.getAndRefreshIfNeeded(context, entityV1, 2, 0).getEntity().getEntityVersion();
    result.r2 =
        entityCache.getAndRefreshIfNeeded(context, entityV1, 2, 0).getEntity().getEntityVersion();
  }
}
