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
package org.apache.polaris.persistence.nosql.quarkus.backend;

import static java.lang.String.format;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import java.util.Optional;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.api.backend.BackendConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
class BackendProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(BackendProvider.class);

  @Produces
  @ApplicationScoped
  @NotObserved
  Backend backend(
      BackendConfiguration backendConfiguration, @Any Instance<BackendBuilder> backendBuilders) {

    var backendName =
        backendConfiguration
            .type()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Mandatory configuration option polaris.persistence.backend.type is missing!"));

    var backendBuilder = backendBuilders.select(BackendType.Literal.of(backendName));
    if (!backendBuilder.isResolvable()) {
      throw new IllegalStateException(
          format(
              "Backend '%s' provided in configuration polaris.persistence.backend.type is not available. Available backends: %s",
              backendName,
              backendBuilders
                  .handlesStream()
                  .map(
                      h ->
                          h.getBean().getQualifiers().stream()
                              .filter(q -> q instanceof BackendType)
                              .map(BackendType.class::cast)
                              .findFirst()
                              .map(BackendType::value))
                  .filter(Optional::isPresent)
                  .map(Optional::get)
                  .toList()));
    }
    if (backendBuilder.isAmbiguous()) {
      throw new IllegalStateException(
          format(
              "Multiple implementations match the backend name '%s' provided in configuration polaris.persistence.backend.type is not available. All available backends: %s",
              backendName,
              backendBuilders
                  .handlesStream()
                  .map(
                      h ->
                          h.getBean().getQualifiers().stream()
                              .filter(q -> q instanceof BackendType)
                              .map(BackendType.class::cast)
                              .findFirst()
                              .map(BackendType::value))
                  .filter(Optional::isPresent)
                  .map(Optional::get)
                  .toList()));
    }
    var builder = backendBuilder.get();

    var backend = builder.buildBackend();
    try {
      var setupSchemaResult = backend.setupSchema().orElse("");
      LOGGER.info("Opened new persistence backend '{}' {}", backend.type(), setupSchemaResult);

      return builder.buildBackend();
    } catch (Exception e) {
      try {
        backend.close();
      } catch (Exception e2) {
        e.addSuppressed(e2);
      }
      throw e;
    }
  }
}
