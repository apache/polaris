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
package org.apache.polaris.core.persistence;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import jakarta.annotation.Nullable;
import java.io.IOException;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PolarisObjectMapperUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisObjectMapperUtil.class);

  private PolarisObjectMapperUtil() {
    // utility class
  }

  public static class TaskExecutionState {
    public final String executor;
    public final long lastAttemptStartTime;
    final int attemptCount;

    TaskExecutionState(String executor, long lastAttemptStartTime, int attemptCount) {
      this.executor = executor;
      this.lastAttemptStartTime = lastAttemptStartTime;
      this.attemptCount = attemptCount;
    }

    public String getExecutor() {
      return executor;
    }

    public long getLastAttemptStartTime() {
      return lastAttemptStartTime;
    }

    public int getAttemptCount() {
      return attemptCount;
    }
  }

  /**
   * Parse a task entity's properties field in order to find the current {@link TaskExecutionState}.
   * Avoids parsing most of the data in the properties field, so we can look at just the fields we
   * need.
   *
   * @param entity entity
   * @return TaskExecutionState
   */
  public static @Nullable TaskExecutionState parseTaskState(PolarisBaseEntity entity) {
    JsonFactory jfactory = new JsonFactory();
    try (JsonParser jParser = jfactory.createParser(entity.getProperties())) {
      String executorId = null;
      long lastAttemptStartTime = 0;
      int attemptCount = 0;
      while (jParser.nextToken() != JsonToken.END_OBJECT) {
        if (jParser.getCurrentToken() == JsonToken.FIELD_NAME) {
          String fieldName = jParser.currentName();
          switch (fieldName) {
            case PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID:
              jParser.nextToken();
              executorId = jParser.getText();
              break;
            case PolarisTaskConstants.LAST_ATTEMPT_START_TIME:
              jParser.nextToken();
              lastAttemptStartTime = Long.parseLong(jParser.getText());
              break;
            case PolarisTaskConstants.ATTEMPT_COUNT:
              jParser.nextToken();
              attemptCount = Integer.parseInt(jParser.getText());
              break;
            default:
              JsonToken next = jParser.nextToken();
              if (next == JsonToken.START_OBJECT || next == JsonToken.START_ARRAY) {
                jParser.skipChildren();
              }
              break;
          }
        }
      }
      return new TaskExecutionState(executorId, lastAttemptStartTime, attemptCount);
    } catch (IOException e) {
      LOGGER
          .atWarn()
          .addKeyValue("json", entity.getProperties())
          .addKeyValue("error", e.getMessage())
          .log("Unable to parse task properties");
      return null;
    }
  }
}
