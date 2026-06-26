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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import org.apache.polaris.admintool.BaseCommand;
import org.apache.polaris.admintool.nosql.maintenance.NoSqlMaintenanceRunCommand;
import org.apache.polaris.persistence.nosql.api.backend.Backend;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceConfig;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunInProgressException;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunInformation;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceRunSpec;
import org.apache.polaris.persistence.nosql.maintenance.api.MaintenanceService;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

public class TestNoSqlMaintenanceRunCommand {
  @Test
  public void passesSupersedeRunIdToMaintenanceService() throws Exception {
    var service = new RecordingMaintenanceService();
    service.runInformation =
        MaintenanceRunInformation.builder()
            .started(Instant.parse("2026-05-11T10:15:30Z"))
            .finished(Instant.parse("2026-05-11T10:15:31Z"))
            .success(true)
            .build();

    var command = new NoSqlMaintenanceRunCommand();
    inject(command, "maintenanceService", service);
    inject(command, "maintenanceConfig", MaintenanceConfig.builder().build());
    inject(command, "backend", backend("MongoDb"));

    var stdout = new StringWriter();
    var stderr = new StringWriter();
    var exitCode =
        new CommandLine(command)
            .setOut(new PrintWriter(stdout, true))
            .setErr(new PrintWriter(stderr, true))
            .execute("--supersede-run=123");

    assertThat(exitCode).isZero();
    assertThat(service.overrideRunId.isPresent()).isTrue();
    assertThat(service.overrideRunId.getAsLong()).isEqualTo(123L);
    assertThat(stderr.toString()).isEmpty();
  }

  @Test
  public void reportsLatestUnfinishedRunIdWhenMaintenanceIsBlocked() throws Exception {
    var service = new RecordingMaintenanceService();
    service.exception =
        new MaintenanceRunInProgressException(
            321L,
            MaintenanceRunInformation.builder()
                .started(Instant.parse("2026-05-11T08:00:00Z"))
                .build());

    var command = new NoSqlMaintenanceRunCommand();
    inject(command, "maintenanceService", service);
    inject(command, "maintenanceConfig", MaintenanceConfig.builder().build());
    inject(command, "backend", backend("MongoDb"));

    var stdout = new StringWriter();
    var stderr = new StringWriter();
    var exitCode =
        new CommandLine(command)
            .setOut(new PrintWriter(stdout, true))
            .setErr(new PrintWriter(stderr, true))
            .execute();

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_MAINTENANCE_ERROR);
    assertThat(service.overrideRunId.isEmpty()).isTrue();
    assertThat(stderr.toString())
        .contains("latest run 321 started at 2026-05-11T08:00:00Z has not finished")
        .contains("--supersede-run=321");
  }

  private static Backend backend(String type) {
    return (Backend)
        Proxy.newProxyInstance(
            Backend.class.getClassLoader(),
            new Class<?>[] {Backend.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "type" -> type;
                case "close" -> null;
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }

  private static void inject(Object target, String fieldName, Object value) throws Exception {
    Class<?> type = target.getClass();
    while (type != null) {
      try {
        Field field = type.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException e) {
        type = type.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }

  static final class RecordingMaintenanceService implements MaintenanceService {
    private final MaintenanceRunSpec runSpec = MaintenanceRunSpec.builder().build();
    private OptionalLong overrideRunId = OptionalLong.empty();
    private MaintenanceRunInformation runInformation;
    private RuntimeException exception;

    @Override
    public @NonNull MaintenanceRunSpec buildMaintenanceRunSpec() {
      return runSpec;
    }

    @Override
    public @NonNull MaintenanceRunInformation performMaintenance(
        @NonNull MaintenanceRunSpec maintenanceRunSpec, @NonNull OptionalLong overrideRunId) {
      this.overrideRunId = overrideRunId;
      if (exception != null) {
        throw exception;
      }
      return runInformation;
    }

    @Override
    public @NonNull List<MaintenanceRunInformation> maintenanceRunLog() {
      return List.of();
    }
  }
}
