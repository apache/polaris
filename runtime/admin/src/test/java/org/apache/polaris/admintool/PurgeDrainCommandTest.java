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

class PurgeDrainCommandTest {

  @Test
  void drainsWithDefaultsAndReportsCount() {
    RecordingFactory factory = new RecordingFactory(3);
    PurgeDrainCommand command = new PurgeDrainCommand();
    command.metaStoreManagerFactory = factory;

    CommandLine commandLine = new CommandLine(command);
    StringWriter out = new StringWriter();
    commandLine.setOut(new PrintWriter(out));

    int exitCode = commandLine.execute();

    assertThat(exitCode).isZero();
    assertThat(out.toString()).contains("Drained 3 realm purge(s).");
    // Default options are passed through to the factory.
    assertThat(factory.batchSize).isEqualTo(1000);
    assertThat(factory.claimLeaseMs).isEqualTo(600_000L);
  }

  @Test
  void drainsWithProvidedOptions() {
    RecordingFactory factory = new RecordingFactory(0);
    PurgeDrainCommand command = new PurgeDrainCommand();
    command.metaStoreManagerFactory = factory;

    CommandLine commandLine = new CommandLine(command);
    commandLine.setOut(new PrintWriter(new StringWriter()));

    int exitCode = commandLine.execute("--batch-size", "250", "--claim-lease-ms", "5000");

    assertThat(exitCode).isZero();
    assertThat(factory.batchSize).isEqualTo(250);
    assertThat(factory.claimLeaseMs).isEqualTo(5000L);
  }

  @Test
  void rejectsNonPositiveBatchSizeWithoutDraining() {
    RecordingFactory factory = new RecordingFactory(0);
    PurgeDrainCommand command = new PurgeDrainCommand();
    command.metaStoreManagerFactory = factory;

    CommandLine commandLine = new CommandLine(command);
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute("--batch-size", "0");

    assertThat(exitCode).isNotZero();
    assertThat(err.toString()).contains("--batch-size must be strictly positive");
    assertThat(factory.batchSize).isEqualTo(-1); // never reached the drainer
  }

  /** Minimal {@link MetaStoreManagerFactory} recording the arguments passed to the drain. */
  private static final class RecordingFactory implements MetaStoreManagerFactory {
    private final int drainResult;
    int batchSize = -1;
    long claimLeaseMs = -1;

    private RecordingFactory(int drainResult) {
      this.drainResult = drainResult;
    }

    @Override
    public int drainRealmPurges(int batchSize, long claimLeaseMs) {
      this.batchSize = batchSize;
      this.claimLeaseMs = claimLeaseMs;
      return drainResult;
    }

    @Override
    public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
      throw new UnsupportedOperationException();
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
