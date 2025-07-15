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
package org.apache.polaris.persistence.nosql.cdi.persistence;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Comparator;
import java.util.List;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.PersistenceDecorator;

/** Applies {@link PersistenceDecorator}s sorted by {@link PersistenceDecorator#priority()}. */
@ApplicationScoped
public class PersistenceDecorators {
  @Inject Instance<PersistenceDecorator> persistenceDecorators;

  private List<PersistenceDecorator> activeDecorators;

  @PostConstruct
  void init() {
    this.activeDecorators =
        persistenceDecorators.stream()
            .filter(PersistenceDecorator::active)
            .sorted(Comparator.comparingInt(PersistenceDecorator::priority))
            .toList();
  }

  public Persistence decorate(Persistence persistence) {
    for (var decorator : activeDecorators) {
      persistence = decorator.decorate(persistence);
    }
    return persistence;
  }
}
