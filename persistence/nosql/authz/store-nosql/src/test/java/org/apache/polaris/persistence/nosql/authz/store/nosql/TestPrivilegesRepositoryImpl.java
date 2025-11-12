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
package org.apache.polaris.persistence.nosql.authz.store.nosql;

import static org.assertj.core.api.InstanceOfAssertFactories.map;

import jakarta.inject.Inject;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegesMapping;
import org.apache.polaris.persistence.nosql.authz.spi.PrivilegesRepository;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jboss.weld.junit5.EnableWeld;
import org.jboss.weld.junit5.WeldInitiator;
import org.jboss.weld.junit5.WeldSetup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
@EnableWeld
public class TestPrivilegesRepositoryImpl {
  @InjectSoftAssertions SoftAssertions soft;
  @WeldSetup WeldInitiator weld = WeldInitiator.performDefaultDiscovery();

  @Inject PrivilegesRepository repository;

  @Test
  public void privilegesRepository() {
    soft.assertThat(repository.fetchPrivilegesMapping())
        .extracting(PrivilegesMapping::nameToId, map(String.class, Integer.class))
        .isEmpty();

    soft.assertThat(
            repository.updatePrivilegesMapping(PrivilegesMapping.EMPTY, PrivilegesMapping.EMPTY))
        .isTrue();
    var initial = PrivilegesMapping.builder().putNameToId("one", 1).putNameToId("two", 2).build();
    soft.assertThat(repository.updatePrivilegesMapping(initial, PrivilegesMapping.EMPTY)).isFalse();
    soft.assertThat(repository.updatePrivilegesMapping(PrivilegesMapping.EMPTY, initial)).isTrue();
    soft.assertThat(repository.updatePrivilegesMapping(PrivilegesMapping.EMPTY, initial)).isFalse();

    var update = PrivilegesMapping.builder().from(initial).putNameToId("three", 3).build();
    soft.assertThat(repository.updatePrivilegesMapping(update, initial)).isFalse();
    soft.assertThat(repository.updatePrivilegesMapping(initial, update)).isTrue();
    soft.assertThat(repository.updatePrivilegesMapping(update, update)).isTrue();
  }
}
