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

import static org.eclipse.persistence.config.PersistenceUnitProperties.JDBC_URL;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.config.ProductionReadinessCheck.Error;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class EclipseLinkProductionReadinessChecks {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(EclipseLinkProductionReadinessChecks.class);

  @Produces
  public ProductionReadinessCheck checkEclipseLink(
      MetaStoreManagerFactory metaStoreManagerFactory,
      EclipseLinkConfiguration eclipseLinkConfiguration) {
    // This check should only be applicable when persistence uses EclipseLink.
    if (!(metaStoreManagerFactory instanceof EclipseLinkPolarisMetaStoreManagerFactory)) {
      return ProductionReadinessCheck.OK;
    }

    try {
      var confFile = eclipseLinkConfiguration.configurationFile().map(Path::toString).orElse(null);
      var persistenceUnitName =
          confFile != null ? eclipseLinkConfiguration.persistenceUnit() : null;
      var unit =
          PolarisEclipseLinkPersistenceUnit.locatePersistenceUnit(confFile, persistenceUnitName);
      var properties = unit.loadProperties();
      var jdbcUrl = properties.get(JDBC_URL);
      if (jdbcUrl != null && jdbcUrl.startsWith("jdbc:h2")) {
        return ProductionReadinessCheck.of(
            Error.of(
                "The current persistence unit (jdbc:h2) is intended for tests only.",
                "polaris.persistence.eclipselink.configuration-file"));
      }
    } catch (IOException e) {
      LOGGER.error("Failed to check JDBC URL", e);
    }
    return ProductionReadinessCheck.OK;
  }
}
