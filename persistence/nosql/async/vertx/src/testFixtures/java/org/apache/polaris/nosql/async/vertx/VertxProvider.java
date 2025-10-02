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
package org.apache.polaris.nosql.async.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class VertxProvider {
  @Produces
  Vertx vertx() {
    return Vertx.vertx(
        new VertxOptions()
            .setInternalBlockingPoolSize(16)
            .setEventLoopPoolSize(16)
            .setWorkerPoolSize(32));
  }

  void dispose(@Disposes Vertx vertx) throws Exception {
    vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.MINUTES);
  }
}
