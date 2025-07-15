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
package org.apache.polaris.persistence.nosql.impl.cache;

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Optional;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.api.obj.ImmutableSimpleTestObj;
import org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj;
import org.apache.polaris.persistence.nosql.impl.AbstractPersistenceTests;
import org.apache.polaris.persistence.nosql.testextension.BackendSpec;
import org.apache.polaris.persistence.nosql.testextension.PersistenceTestExtension;
import org.apache.polaris.persistence.nosql.testextension.PolarisPersistence;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({PersistenceTestExtension.class, SoftAssertionsExtension.class})
@BackendSpec
public class TestCachingInMemoryPersist extends AbstractPersistenceTests {
  @PolarisPersistence(caching = true)
  protected Persistence persistence;

  @Override
  protected Persistence persistence() {
    return persistence;
  }

  @Nested
  @ExtendWith({PersistenceTestExtension.class, SoftAssertionsExtension.class})
  public class CacheSpecific {
    @InjectSoftAssertions protected SoftAssertions soft;

    @PolarisPersistence(caching = true)
    protected Persistence persist;

    @PolarisPersistence protected IdGenerator idGenerator;

    @Test
    public void getImmediate() {
      var obj = ImmutableSimpleTestObj.builder().id(idGenerator.generateId()).text("foo").build();
      soft.assertThat(persist.getImmediate(objRef(obj.withNumParts(1)), SimpleTestObj.class))
          .isNull();
      var written = persist.write(obj, SimpleTestObj.class);
      soft.assertThat(persist.getImmediate(objRef(written), SimpleTestObj.class))
          .isEqualTo(written);
      persist.delete(objRef(written));
      soft.assertThat(persist.getImmediate(objRef(written), SimpleTestObj.class)).isNull();
      persist.write(obj, SimpleTestObj.class);
      persist.fetch(objRef(written), SimpleTestObj.class);
      soft.assertThat(persist.getImmediate(objRef(written), SimpleTestObj.class))
          .isEqualTo(written);
    }

    @Test
    public void nonEffectiveNegativeCache(@PolarisPersistence Persistence persist) {
      var backing = spy(persist);
      var cacheBackend =
          PersistenceCaches.newBackend(
              CacheConfig.BuildableCacheConfig.builder()
                  .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(16)).build())
                  .build(),
              Optional.empty());
      var cachedPersist = spy(cacheBackend.wrap(backing));

      reset(backing);

      var id = objRef(SimpleTestObj.TYPE, idGenerator.generateId(), 1);

      soft.assertThat(cachedPersist.fetch(id, SimpleTestObj.class)).isNull();
      verify(cachedPersist).fetch(id, SimpleTestObj.class);
      verify(backing).fetch(id, SimpleTestObj.class);
      verify(backing).fetchMany(same(SimpleTestObj.class), any());
      // BasePersistence calls 'doFetch()', which is protected and not accessible from this test
      // verifyNoMoreInteractions(backing);
      verifyNoMoreInteractions(cachedPersist);
      reset(backing, cachedPersist);

      // repeat
      soft.assertThat(cachedPersist.fetch(id, SimpleTestObj.class)).isNull();
      verify(cachedPersist).fetch(id, SimpleTestObj.class);
      verify(backing).fetch(id, SimpleTestObj.class);
      verify(backing).fetchMany(same(SimpleTestObj.class), any());
      // BasePersistence calls 'doFetch()', which is protected and not accessible from this test
      // verifyNoMoreInteractions(backing);
      verifyNoMoreInteractions(cachedPersist);
      reset(backing, cachedPersist);
    }
  }
}
