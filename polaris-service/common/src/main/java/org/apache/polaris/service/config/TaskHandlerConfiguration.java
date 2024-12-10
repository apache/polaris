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
package org.apache.polaris.service.config;

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
