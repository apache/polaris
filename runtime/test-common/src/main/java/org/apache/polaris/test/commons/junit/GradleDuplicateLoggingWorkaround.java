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

package org.apache.polaris.test.commons.junit;

import jakarta.annotation.Priority;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Startup;
import jakarta.inject.Singleton;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit extension + Singleton CDI bean that "fixes" test logging configuration.
 *
 * <p>This component removes the SLF4JBridgeHandler installed by Gradle, to prevent duplicate
 * logging messages in tests. Without this fix, Gradle tests using JBoss LogManager (which is the
 * default in Quarkus) will log each message twice: once via the SLF4JBridgeHandler, without any
 * formatting, and once via the JBoss LogManager (with formatting). This is annoying because the
 * non-formatted messages appear on the console, regardless of the log level.
 *
 * <p>Note: this issue has been reported to Quarkus and appears fixed, but only the formatting part
 * is fixed, not the duplicate messages.
 *
 * @see <a href="https://github.com/quarkusio/quarkus/issues/22844">Gradle tests (with JBoss
 *     LogManager setup) output duplicate unformatted messages</a>
 * @see <a href="https://github.com/quarkusio/quarkus/issues/48763">Gradle
 *     testLogging.showStandardStreams = false ignored by Quarkus tests with JBoss LogManager</a>
 */
@Singleton
public class GradleDuplicateLoggingWorkaround implements BeforeAllCallback {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GradleDuplicateLoggingWorkaround.class);

  private static final AtomicBoolean DONE = new AtomicBoolean(false);

  public void onStartup(@Observes @Priority(1) Startup event) {
    // Sometimes the application is started before the test extension is invoked,
    // so we need to ensure the SLF4JBridgeHandler is removed at startup as well.
    removeGradleHandlers();
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    removeGradleHandlers();
  }

  private static void removeGradleHandlers() {
    if (!DONE.getAndSet(true)) {
      var logger = org.jboss.logmanager.LogManager.getLogManager().getLogger("");
      var found = false;
      for (var handler : logger.getHandlers()) {
        if (handler.getClass().getSimpleName().equals("SLF4JBridgeHandler")) {
          logger.removeHandler(handler);
          found = true;
        }
      }
      if (found) {
        LOGGER.warn(
            "Removed SLF4JBridgeHandler to fix duplicate logging messages in Gradle tests.");
      }
    }
  }
}
