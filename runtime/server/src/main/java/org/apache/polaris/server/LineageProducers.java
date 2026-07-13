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
package org.apache.polaris.server;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.extensions.lineage.LineageConfiguration;
import org.apache.polaris.extensions.lineage.LineageStoreManager;
import org.apache.polaris.extensions.lineage.NoopLineageStoreManager;

@ApplicationScoped
public class LineageProducers {
  private static final String LINEAGE_PERSISTENCE_TYPE_NOOP = "noop";
  private static final String LINEAGE_PERSISTENCE_TYPE_RELATIONAL_JDBC = "relational-jdbc";

  @Produces
  @RequestScoped
  LineageStoreManager produceLineageStoreManager(
      RealmContext realmContext,
      MetaStoreManagerFactory metaStoreManagerFactory,
      LineageConfiguration lineageConfiguration) {
    String persistenceType = lineageConfiguration.persistence().type();
    if (LINEAGE_PERSISTENCE_TYPE_NOOP.equals(persistenceType)) {
      return new NoopLineageStoreManager();
    }
    if (!LINEAGE_PERSISTENCE_TYPE_RELATIONAL_JDBC.equals(persistenceType)) {
      throw new UnsupportedOperationException(
          "Unsupported lineage persistence type: " + persistenceType);
    }
    BasePersistence session = metaStoreManagerFactory.getOrCreateSession(realmContext);
    if (session instanceof LineageStoreManager lineageStoreManager) {
      return lineageStoreManager;
    }
    throw new UnsupportedOperationException(
        "Lineage persistence type is relational-jdbc, but the configured session does not "
            + "implement "
            + LineageStoreManager.class.getSimpleName()
            + ".");
  }
}
