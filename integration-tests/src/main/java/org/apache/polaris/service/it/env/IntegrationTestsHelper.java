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
import com.google.common.collect.ImmutableMap;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.TestInfo;

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

  /**
   * Extract a value from the annotated elements of the test method and class.
   *
   * <p>This method looks for annotations of the specified type on both the test method and the test
   * class, extracts the value using the provided extractor function, and returns it.
   *
   * <p>If the value is present in both the method and class annotations, the value from the method
   * annotation will be used. If the value is not present in either the method or class annotations,
   * this method returns the default value.
   */
  public static <A extends Annotation, T> T extractFromAnnotatedElements(
      TestInfo testInfo, Class<A> annotationClass, Function<A, T> extractor, T defaultValue) {
    return testInfo
        .getTestMethod()
        .map(m -> m.getAnnotation(annotationClass))
        .or(() -> testInfo.getTestClass().map(c -> c.getAnnotation(annotationClass)))
        .map(extractor)
        .orElse(defaultValue);
  }

  /**
   * Collect properties from annotated elements in the test method and class.
   *
   * <p>This method looks for annotations of the specified type on both the test method and the test
   * class, extracts properties using the provided extractor function, and combines them into a map.
   * If a property appears in both the method and class annotations, the value from the method
   * annotation will be used.
   */
  public static <A extends Annotation> Map<String, String> mergeFromAnnotatedElements(
      TestInfo testInfo, Class<A> annotationClass, Function<A, String[]> propertiesExtractor) {
    String[] methodProperties =
        testInfo
            .getTestMethod()
            .map(m -> m.getAnnotation(annotationClass))
            .map(propertiesExtractor)
            .orElse(new String[0]);
    String[] classProperties =
        testInfo
            .getTestClass()
            .map(clz -> clz.getAnnotation(annotationClass))
            .map(propertiesExtractor)
            .orElse(new String[0]);
    String[] properties =
        Stream.concat(Arrays.stream(classProperties), Arrays.stream(methodProperties))
            .toArray(String[]::new);
    if (properties.length % 2 != 0) {
      throw new IllegalArgumentException(
          "Properties must be in key-value pairs, but found an odd number of elements: "
              + Arrays.toString(properties));
    }
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (int i = 0; i < properties.length; i += 2) {
      builder.put(properties[i], properties[i + 1]);
    }
    return builder.buildKeepingLast();
  }
}
