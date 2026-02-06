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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IntegrationTestsHelperTest {

  @ParameterizedTest
  @MethodSource
  void getTemporaryDirectory(String envVar, Path local, URI expected) {
    URI actual = IntegrationTestsHelper.getTemporaryDirectory(s -> envVar, local);
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> getTemporaryDirectory() {
    return Stream.of(
        Arguments.of(null, Path.of("/tmp/polaris"), URI.create("file:///tmp/polaris/")),
        Arguments.of(null, Path.of("/tmp/polaris/"), URI.create("file:///tmp/polaris/")),
        Arguments.of(
            "file:///tmp/polaris/from-env",
            Path.of("/tmp/polaris/default"),
            URI.create("file:///tmp/polaris/from-env/")),
        Arguments.of(
            "file:///tmp/polaris/from-env/",
            Path.of("/tmp/polaris/default"),
            URI.create("file:///tmp/polaris/from-env/")),
        Arguments.of(
            "/tmp/polaris/from-env",
            Path.of("/tmp/polaris/default"),
            URI.create("file:///tmp/polaris/from-env/")),
        Arguments.of(
            "/tmp/polaris/from-env/",
            Path.of("/tmp/polaris/default"),
            URI.create("file:///tmp/polaris/from-env/")),
        Arguments.of(
            "s3://bucket/polaris/from-env",
            Path.of("/tmp/polaris/default"),
            URI.create("s3://bucket/polaris/from-env/")),
        Arguments.of(
            "s3://bucket/polaris/from-env/",
            Path.of("/tmp/polaris/default"),
            URI.create("s3://bucket/polaris/from-env/")));
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  @interface TestAnnotation {
    String value() default "";

    String[] properties() default {};
  }

  @TestAnnotation(
      value = "classValue",
      properties = {"sharedKey", "classValue", "classKey", "classValue"})
  static class AnnotatedClass {
    @TestAnnotation(
        value = "methodValue",
        properties = {"sharedKey", "methodValue", "methodKey", "methodValue"})
    public void annotatedMethod() {}
  }

  static class UnannotatedClass {
    public void unannotatedMethod() {}
  }

  @ParameterizedTest
  @MethodSource
  void extractFromAnnotatedElements(Method testMethod, Class<?> testClass, String expected) {
    TestInfo testInfo = mock(TestInfo.class);
    when(testInfo.getTestMethod()).thenReturn(Optional.ofNullable(testMethod));
    when(testInfo.getTestClass()).thenReturn(Optional.ofNullable(testClass));

    String result =
        IntegrationTestsHelper.extractFromAnnotatedElements(
            testInfo, TestAnnotation.class, TestAnnotation::value, "defaultValue");

    assertThat(result).isEqualTo(expected);
  }

  static Stream<Arguments> extractFromAnnotatedElements() throws Exception {
    Method annotatedMethod = AnnotatedClass.class.getMethod("annotatedMethod");
    Method unannotatedMethod = UnannotatedClass.class.getMethod("unannotatedMethod");

    return Stream.of(
        // Method has annotation - use method value
        Arguments.of(annotatedMethod, AnnotatedClass.class, "methodValue"),
        // Method has no annotation, class has annotation - fall back to class value
        Arguments.of(unannotatedMethod, AnnotatedClass.class, "classValue"),
        // No method, class has annotation - use class value
        Arguments.of(null, AnnotatedClass.class, "classValue"),
        // Method has no annotation, class has no annotation - use default
        Arguments.of(unannotatedMethod, UnannotatedClass.class, "defaultValue"),
        // No method, no class - use default
        Arguments.of(null, null, "defaultValue"),
        // No method, class has no annotation - use default
        Arguments.of(null, UnannotatedClass.class, "defaultValue"));
  }

  @ParameterizedTest
  @MethodSource
  void mergeFromAnnotatedElements(
      Method testMethod, Class<?> testClass, Map<String, String> expected) {
    TestInfo testInfo = mock(TestInfo.class);
    when(testInfo.getTestMethod()).thenReturn(Optional.ofNullable(testMethod));
    when(testInfo.getTestClass()).thenReturn(Optional.ofNullable(testClass));

    Map<String, String> result =
        IntegrationTestsHelper.mergeFromAnnotatedElements(
            testInfo, TestAnnotation.class, TestAnnotation::properties);

    assertThat(result).isEqualTo(expected);
  }

  static Stream<Arguments> mergeFromAnnotatedElements() throws Exception {
    Method annotatedMethod = AnnotatedClass.class.getMethod("annotatedMethod");
    Method unannotatedMethod = UnannotatedClass.class.getMethod("unannotatedMethod");

    return Stream.of(
        // Both method and class have properties - merge with method overriding shared key
        Arguments.of(
            annotatedMethod,
            AnnotatedClass.class,
            Map.of(
                "sharedKey", "methodValue", "classKey", "classValue", "methodKey", "methodValue")),
        // Only method has properties
        Arguments.of(
            annotatedMethod,
            UnannotatedClass.class,
            Map.of("sharedKey", "methodValue", "methodKey", "methodValue")),
        // Only class has properties
        Arguments.of(
            unannotatedMethod,
            AnnotatedClass.class,
            Map.of("sharedKey", "classValue", "classKey", "classValue")),
        // No method, class has properties
        Arguments.of(
            null,
            AnnotatedClass.class,
            Map.of("sharedKey", "classValue", "classKey", "classValue")),
        // Neither has properties - return empty
        Arguments.of(unannotatedMethod, UnannotatedClass.class, Map.of()),
        // No method, no class - return empty
        Arguments.of(null, null, Map.of()));
  }

  @TestAnnotation(properties = {"key1", "value1", "key2"})
  static class OddPropertiesClass {
    public void method() {}
  }

  @Test
  void mergeFromAnnotatedElementsThrowsOnOddNumberOfProperties() throws Exception {
    Method method = OddPropertiesClass.class.getMethod("method");
    TestInfo testInfo = mock(TestInfo.class);
    when(testInfo.getTestMethod()).thenReturn(Optional.of(method));
    when(testInfo.getTestClass()).thenReturn(Optional.of(OddPropertiesClass.class));

    assertThatThrownBy(
            () ->
                IntegrationTestsHelper.mergeFromAnnotatedElements(
                    testInfo, TestAnnotation.class, TestAnnotation::properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Properties must be in key-value pairs");
  }
}
