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
package org.apache.polaris.service.persistence;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.persistence.AtomicOperationMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;

/**
 * Uses a PolarisTreeMapStore for the underlying persistence layer but uses it to initialize an
 * AtomicOperationMetaStoreManager instead of the transactional version.
 */
@ApplicationScoped
@Identifier("in-memory-atomic")
public class InMemoryAtomicOperationMetaStoreManagerFactory
    extends InMemoryPolarisMetaStoreManagerFactory {

  @SuppressWarnings("unused") // Required by CDI
  protected InMemoryAtomicOperationMetaStoreManagerFactory() {
    this(null, null);
  }

  @Inject
  public InMemoryAtomicOperationMetaStoreManagerFactory(
      Clock clock, PolarisDiagnostics diagnostics) {
    super(clock, diagnostics);
  }

  @Override
  protected PolarisMetaStoreManager createNewMetaStoreManager(Clock clock) {
    return new AtomicOperationMetaStoreManager(clock);
  }
}
