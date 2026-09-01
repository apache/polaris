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
package org.apache.polaris.service.storage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import org.apache.polaris.core.storage.r2.R2ParentToken;
import org.apache.polaris.core.storage.r2.R2ParentTokenResolver;
import org.jspecify.annotations.Nullable;

/** CDI adapter exposing {@link StorageConfiguration#r2ParentTokenResolver()} as a bean. */
@ApplicationScoped
public class R2ParentTokenResolverImpl implements R2ParentTokenResolver {

  private final R2ParentTokenResolver delegate;

  @Inject
  public R2ParentTokenResolverImpl(StorageConfiguration storageConfiguration) {
    this.delegate = storageConfiguration.r2ParentTokenResolver();
  }

  @Override
  public Optional<R2ParentToken> resolve(@Nullable String storageName) {
    return delegate.resolve(storageName);
  }
}
