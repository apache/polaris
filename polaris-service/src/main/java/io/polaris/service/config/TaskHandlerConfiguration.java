package io.polaris.service.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class TaskHandlerConfiguration {
  private int poolSize = 10;
  private boolean fixedSize = true;
  private String threadNamePattern = "taskHandler-%d";

  public void setPoolSize(int poolSize) {
    this.poolSize = poolSize;
  }

  public void setFixedSize(boolean fixedSize) {
    this.fixedSize = fixedSize;
  }

  public void setThreadNamePattern(String threadNamePattern) {
    this.threadNamePattern = threadNamePattern;
  }

  public ExecutorService executorService() {
    return fixedSize
        ? Executors.newFixedThreadPool(poolSize, threadFactory())
        : Executors.newCachedThreadPool(threadFactory());
  }

  private ThreadFactory threadFactory() {
    return new ThreadFactoryBuilder().setNameFormat(threadNamePattern).setDaemon(true).build();
  }
}
