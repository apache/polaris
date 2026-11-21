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
package org.apache.polaris.admintool.nosql;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.apache.polaris.admintool.AdminProfiles;
import org.apache.polaris.admintool.BaseCommand;
import org.junit.jupiter.api.Test;

/**
 * The drain is specific to the relational-JDBC backend. On another backend the command must report
 * a usage error rather than fail: the JDBC beans it injects are on the classpath but have no
 * datasource to resolve against, so this also pins that their absence does not break startup.
 */
@QuarkusMainTest
@TestProfile(AdminProfiles.NoSqlInMemory.class)
class NoSqlInMemoryPurgeDrainCommandTest {

  @Test
  @Launch(value = "purge-drain", exitCode = BaseCommand.EXIT_CODE_USAGE)
  void purgeDrainIsRejectedOnNoSqlPersistence(LaunchResult result) {
    assertThat(result.getErrorOutput()).contains("purge-drain applies to the 'relational-jdbc'");
  }
}
