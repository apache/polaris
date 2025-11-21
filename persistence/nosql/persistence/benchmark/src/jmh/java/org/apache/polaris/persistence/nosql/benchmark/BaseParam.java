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
package org.apache.polaris.persistence.nosql.benchmark;

import static java.util.function.Function.identity;

import java.util.Map;
import org.apache.polaris.ids.api.MonotonicClock;
import org.apache.polaris.ids.impl.MonotonicClockImpl;
import org.apache.polaris.ids.impl.SnowflakeIdGeneratorFactory;
import org.apache.polaris.ids.spi.IdGeneratorSource;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceParams;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.standalone.PersistenceConfigurer;

class BaseParam {
  Backend backend;
  Persistence persistence;
  MonotonicClock clock;

  void setupPersistence() {
    var configurer = PersistenceConfigurer.defaultBackendConfigurer();
    var factory = configurer.buildBackendFactory();
    this.clock = MonotonicClockImpl.newDefaultInstance();
    this.backend = configurer.buildBackendFromConfiguration(factory);
    var info = backend.setupSchema().orElse("");
    System.out.printf("Opened new persistence backend '%s' %s%n", backend.type(), info);

    var idGenerator =
        new SnowflakeIdGeneratorFactory()
            .buildIdGenerator(
                Map.of(),
                new IdGeneratorSource() {
                  @Override
                  public int nodeId() {
                    return 42;
                  }

                  @Override
                  public long currentTimeMillis() {
                    return clock.currentTimeMillis();
                  }
                });
    this.persistence =
        backend.newPersistence(
            identity(),
            PersistenceParams.BuildablePersistenceParams.builder().build(),
            "42",
            clock,
            idGenerator);

    // TODO allow caching
  }

  void shutdownPersistence() throws Exception {
    if (clock != null) {
      clock.close();
    }
    if (backend != null) {
      backend.close();
    }
  }
}
