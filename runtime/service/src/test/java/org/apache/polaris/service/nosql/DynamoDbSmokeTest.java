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
package org.apache.polaris.service.nosql;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.UUID;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.SystemPersistence;
import org.apache.polaris.service.Profiles;
import org.junit.jupiter.api.Test;

@QuarkusTest
@QuarkusTestResource(value = DynamoDbTestResource.class, restrictToAnnotatedClass = true)
@TestProfile(Profiles.DefaultProfile.class)
class DynamoDbSmokeTest {

  @Inject @SystemPersistence Persistence persistence;

  @Test
  void persistsReferenceThroughServiceRuntime() {
    var referenceName = "runtime-smoke-" + UUID.randomUUID();
    var createdReference = persistence.createReference(referenceName, Optional.empty());
    var fetchedReference = persistence.fetchReference(referenceName);

    assertThat(fetchedReference.name()).isEqualTo(referenceName);
    assertThat(fetchedReference.pointer()).isEmpty();
    assertThat(fetchedReference.createdAtMicros()).isEqualTo(createdReference.createdAtMicros());
  }
}
