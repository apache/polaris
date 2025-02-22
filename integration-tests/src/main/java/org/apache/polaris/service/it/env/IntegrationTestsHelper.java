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
package org.apache.polaris.service.it.env;

import com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.nio.file.Path;
import java.util.function.Function;

public final class IntegrationTestsHelper {

  /**
   * The environment variable that can be used to override the temporary directory used by the
   * integration tests.
   */
  public static final String INTEGRATION_TEST_TEMP_DIR_ENV_VAR = "INTEGRATION_TEST_TEMP_DIR";

  private IntegrationTestsHelper() {}

  /**
   * Get the temporary directory to use for integration tests.
   *
   * <p>If the environment variable {@link #INTEGRATION_TEST_TEMP_DIR_ENV_VAR} is set, it will be
   * used as the temporary directory. Otherwise, the default local temporary directory will be used.
   *
   * <p>The environment variable should be a URI, e.g. {@code "file:///tmp/polaris"} or {@code
   * "s3://bucket/polaris"}. If the URI does not have a scheme, it will be assumed to be a local
   * file URI.
   */
  public static URI getTemporaryDirectory(Path defaultLocalDirectory) {
    return getTemporaryDirectory(System::getenv, defaultLocalDirectory);
  }

  @VisibleForTesting
  static URI getTemporaryDirectory(Function<String, String> getenv, Path defaultLocalDirectory) {
    String envVar = getenv.apply(INTEGRATION_TEST_TEMP_DIR_ENV_VAR);
    envVar = envVar != null ? envVar : defaultLocalDirectory.toString();
    envVar = envVar.startsWith("/") ? "file://" + envVar : envVar;
    return URI.create(envVar + "/").normalize();
  }
}
