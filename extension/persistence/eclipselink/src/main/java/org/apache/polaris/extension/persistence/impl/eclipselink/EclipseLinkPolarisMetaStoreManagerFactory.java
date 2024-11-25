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
package org.apache.polaris.extension.persistence.impl.eclipselink;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.LocalPolarisMetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.jetbrains.annotations.NotNull;

/**
 * The implementation of Configuration interface for configuring the {@link PolarisMetaStoreManager}
 * using an EclipseLink based meta store to store and retrieve all Polaris metadata. It can be
 * configured through persistence.xml to use supported RDBMS as the meta store.
 */
@JsonTypeName("eclipse-link")
public class EclipseLinkPolarisMetaStoreManagerFactory
    extends LocalPolarisMetaStoreManagerFactory<PolarisEclipseLinkStore> {
  @JsonProperty("conf-file")
  private String confFile;

  @JsonProperty("persistence-unit")
  private String persistenceUnitName;

  @Override
  protected PolarisEclipseLinkStore createBackingStore(@NotNull PolarisDiagnostics diagnostics) {
    return new PolarisEclipseLinkStore(diagnostics);
  }

  @Override
  protected PolarisMetaStoreSession createMetaStoreSession(
      @NotNull PolarisEclipseLinkStore store, @NotNull RealmContext realmContext) {
    return new PolarisEclipseLinkMetaStoreSessionImpl(
        store,
        storageIntegration,
        realmContext,
        confFile,
        persistenceUnitName,
        secretsGenerator(realmContext));
  }
}
