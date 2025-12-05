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

package org.apache.polaris.service.task;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.function.TriConsumer;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

@ApplicationScoped
@Identifier("task-error-handler")
public class TaskErrorHandler implements TriConsumer<Long, Boolean, Throwable> {

  private final ConcurrentMap<Long, TaskStatus> tasks = new ConcurrentHashMap<>();

  private record TaskStatus(boolean completed, Throwable error) {}

  @Override
  public void accept(Long id, Boolean start, Throwable th) {
    if (start) {
      tasks.computeIfAbsent(id, x -> new TaskStatus(false, null));
    } else {
      tasks.compute(id, (i, s) -> new TaskStatus(true, th != null || s == null ? th : s.error));
    }
  }

  public void assertNoTaskExceptions() {
    List<Long> ids = new ArrayList<>(tasks.keySet());
    TaskStatus incomplete = new TaskStatus(false, null);
    Awaitility.await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> ids.stream().allMatch(id -> tasks.getOrDefault(id, incomplete).completed));

    for (Long id : ids) {
      TaskStatus s = tasks.remove(id);
      Assertions.assertThatCode(
              () -> {
                if (s.error != null) {
                  throw s.error;
                }
              })
          .doesNotThrowAnyException();
      Assertions.assertThat(s.completed).isTrue();
    }
  }
}
