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
