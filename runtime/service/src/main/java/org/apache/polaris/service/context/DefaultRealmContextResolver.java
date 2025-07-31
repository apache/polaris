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
package org.apache.polaris.service.context;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.apache.polaris.core.context.RealmContext;

@ApplicationScoped
@Identifier("default")
public class DefaultRealmContextResolver implements RealmContextResolver {

  private final RealmContextConfiguration configuration;

  @Inject
  public DefaultRealmContextResolver(RealmContextConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public CompletionStage<RealmContext> resolveRealmContext(
      String requestURL, String method, String path, Function<String, String> headers) {
    try {
      String realm = resolveRealmIdentifier(headers);
      return CompletableFuture.completedFuture(() -> realm);
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private String resolveRealmIdentifier(Function<String, String> headers) {
    String realm = headers.apply(configuration.headerName());
    if (realm != null) {
      if (!configuration.realms().contains(realm)) {
        throw new IllegalArgumentException("Unknown realm: " + realm);
      }
    } else {
      if (configuration.requireHeader()) {
        throw new IllegalArgumentException(
            "Missing required realm header: " + configuration.headerName());
      }
      realm = configuration.defaultRealm();
    }
    return realm;
  }
}
