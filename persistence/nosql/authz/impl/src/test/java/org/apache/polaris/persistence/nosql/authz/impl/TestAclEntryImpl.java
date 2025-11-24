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
package org.apache.polaris.persistence.nosql.authz.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.authz.api.AclEntry;
import org.apache.polaris.persistence.nosql.authz.api.Privilege;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestAclEntryImpl {
  @InjectSoftAssertions SoftAssertions soft;

  private static ObjectMapper mapper;
  private static PrivilegesImpl privileges;

  @BeforeAll
  static void setUp() {
    mapper = new ObjectMapper().findAndRegisterModules();
    privileges =
        new PrivilegesImpl(Stream.of(new PrivilegesTestProvider()), new PrivilegesTestRepository());
    JacksonPrivilegesModule.CDIResolver.setResolver(x -> privileges);
  }

  @ParameterizedTest
  @MethodSource
  public void aclEntry(AclEntry aclEntry) throws Exception {

    String json = mapper.writeValueAsString(aclEntry);
    soft.assertThat(mapper.readValue(json, AclEntry.class)).isEqualTo(aclEntry);
  }

  static Stream<AclEntry> aclEntry() {
    Privilege zero = privileges.byId(0);
    Privilege eight = privileges.byId(8);
    Privilege nine = privileges.byId(9);
    return Stream.of(
        privileges.newAclEntryBuilder().build(),
        privileges.newAclEntryBuilder().grant(zero).build(),
        privileges.newAclEntryBuilder().restrict(zero).build(),
        privileges.newAclEntryBuilder().grant(zero).restrict(zero).build(),
        privileges.newAclEntryBuilder().grant(zero, eight, nine).build(),
        privileges.newAclEntryBuilder().restrict(zero, eight, nine).build(),
        privileges
            .newAclEntryBuilder()
            .grant(zero, eight, nine)
            .restrict(zero, eight, nine)
            .build());
  }
}
