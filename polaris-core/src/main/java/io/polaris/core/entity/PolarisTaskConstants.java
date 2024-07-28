/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.core.entity;

/** Constants used to store task properties and configuration parameters */
public class PolarisTaskConstants {
  public static final long TASK_TIMEOUT_MILLIS = 300000;
  public static final String TASK_TIMEOUT_MILLIS_CONFIG = "POLARIS_TASK_TIMEOUT_MILLIS";
  public static final String LAST_ATTEMPT_EXECUTOR_ID = "lastAttemptExecutorId";
  public static final String LAST_ATTEMPT_START_TIME = "lastAttemptStartTime";
  public static final String ATTEMPT_COUNT = "attemptCount";
  public static final String TASK_DATA = "data";
  public static final String TASK_TYPE = "taskType";
  public static final String STORAGE_LOCATION = "storageLocation";
}
