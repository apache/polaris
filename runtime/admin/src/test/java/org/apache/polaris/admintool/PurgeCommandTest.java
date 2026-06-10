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
package org.apache.polaris.admintool;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class PurgeCommandTest {

  /**
   * When {@code purgeRealms} fails (e.g. database connection failure, permission denied), the
   * command must surface the underlying exception to stderr so operators can diagnose it, in
   * addition to the generic error message and the purge error exit code.
   */
  @Test
  void testPurgeFailurePrintsStackTrace() {
    String failureMessage = "simulated database connection failure";
    PurgeCommand command = new PurgeCommand();
    command.metaStoreManagerFactory =
        new ThrowingMetaStoreManagerFactory(new RuntimeException(failureMessage));

    CommandLine commandLine = new CommandLine(command);
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute("-r", "realm1");

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_PURGE_ERROR);
    assertThat(err.toString())
        .contains("java.lang.RuntimeException: " + failureMessage)
        .contains("\tat ")
        .contains("Purge encountered errors during operation.");
  }

  /** Minimal {@link MetaStoreManagerFactory} whose {@code purgeRealms} always throws. */
  private static final class ThrowingMetaStoreManagerFactory implements MetaStoreManagerFactory {
    private final RuntimeException failure;

    private ThrowingMetaStoreManagerFactory(RuntimeException failure) {
      this.failure = failure;
    }

    @Override
    public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
      throw failure;
    }

    @Override
    public PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BasePersistence getOrCreateSession(RealmContext realmContext) {
      throw new UnsupportedOperationException();
    }

    @Override
    public EntityCache getOrCreateEntityCache(RealmContext realmContext, RealmConfig realmConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, PrincipalSecretsResult> bootstrapRealms(
        Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
      throw new UnsupportedOperationException();
    }
  }
}
