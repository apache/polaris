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
package org.apache.polaris.service.admin;

import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.polaris.core.admin.model.Catalog;

/**
 * Catalog configuration validators installed in this server, resolved once at startup. Only bean
 * references are cached; validators read contextual configuration when validation is invoked.
 */
@ApplicationScoped
@Startup
public class CatalogConfigValidators {
  private final List<CatalogConfigValidator> validators;

  @Inject
  public CatalogConfigValidators(@Any Instance<CatalogConfigValidator> candidates) {
    this(candidates.stream().toList());
  }

  public CatalogConfigValidators(List<CatalogConfigValidator> validators) {
    this.validators = List.copyOf(validators);
  }

  public void validate(Catalog catalog) {
    for (CatalogConfigValidator validator : validators) {
      validator.validate(catalog);
    }
  }
}
