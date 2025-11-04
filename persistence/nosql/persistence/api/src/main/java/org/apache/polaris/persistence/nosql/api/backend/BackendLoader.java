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
package org.apache.polaris.persistence.nosql.api.backend;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class BackendLoader {
  private BackendLoader() {}

  @Nonnull
  public static <C> BackendFactory<C, ?> findFactoryByName(@Nonnull String name) {
    return findFactory(f -> f.name().equals(name));
  }

  @Nonnull
  public static Stream<BackendFactory<?, ?>> availableFactories() {
    @SuppressWarnings("rawtypes")
    var x = (Stream) loader().stream().map(ServiceLoader.Provider::get);
    @SuppressWarnings("unchecked")
    var r = (Stream<BackendFactory<?, ?>>) x;
    return r;
  }

  @Nonnull
  public static <C> BackendFactory<C, ?> findFactory(
      @Nonnull Predicate<BackendFactory<?, ?>> filter) {
    ServiceLoader<BackendFactory<C, ?>> loader = loader();
    List<BackendFactory<?, ?>> candidates = new ArrayList<>();
    boolean any = false;
    for (BackendFactory<C, ?> backendFactory : loader) {
      any = true;
      if (filter.test(backendFactory)) {
        candidates.add(backendFactory);
      }
    }
    checkState(any, "No BackendFactory on class path");
    checkArgument(!candidates.isEmpty(), "No BackendFactory matched the given filter");

    if (candidates.size() == 1) {
      return cast(candidates.getFirst());
    }

    throw new IllegalStateException(
        "More than one BackendFactory matched the given filter: "
            + candidates.stream().map(BackendFactory::name).collect(Collectors.joining(", ")));
  }

  // Helper for ugly generics casting
  private static <C> ServiceLoader<BackendFactory<C, ?>> loader() {
    @SuppressWarnings("rawtypes")
    ServiceLoader<BackendFactory> f = ServiceLoader.load(BackendFactory.class);
    @SuppressWarnings({"unchecked", "rawtypes"})
    ServiceLoader<BackendFactory<C, ?>> r = (ServiceLoader) f;
    return r;
  }

  // Helper for ugly generics casting
  private static <C> BackendFactory<C, ?> cast(BackendFactory<?, ?> backendFactory) {
    @SuppressWarnings("unchecked")
    BackendFactory<C, ?> r = (BackendFactory<C, ?>) backendFactory;
    return r;
  }
}
