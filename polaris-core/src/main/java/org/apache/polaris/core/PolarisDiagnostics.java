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
package org.apache.polaris.core;

public interface PolarisDiagnostics {

  /**
   * Fail with an exception
   *
   * @param signature signature, small unique string to identify this assertion within the method,
   *     like "path_cannot_be_null"
   * @param extraInfoFormat extra information regarding the assertion. Generally a set of name/value
   *     pairs: "id={} fileName={}"
   * @param extraInfoArgs extra information arguments
   */
  RuntimeException fail(
      final String signature, final String extraInfoFormat, final Object... extraInfoArgs);

  /**
   * Fail because of an exception
   *
   * @param signature signature, small unique string to identify this assertion within the method,
   *     like "path_cannot_be_null"
   * @param cause exception which cause the issue
   * @param extraInfoFormat extra information regarding the assertion. Generally a set of name/value
   *     pairs: "id={} fileName={}"
   * @param extraInfoArgs extra information arguments
   */
  RuntimeException fail(
      final String signature,
      final Throwable cause,
      final String extraInfoFormat,
      final Object... extraInfoArgs);

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null
   *
   * @param reference an object reference
   * @param signature signature, small unique string to identify this assertion within the method,
   *     like "path_cannot_be_null"
   * @return the non-null reference that was validated
   * @throws RuntimeException if `reference` is null
   */
  <T> T checkNotNull(final T reference, final String signature);

  /**
   * Ensures that an object reference passed as a parameter to the calling method is not null
   *
   * @param reference an object reference
   * @param signature signature, small unique string to identify this assertion within the method,
   *     like "path_cannot_be_null"
   * @param extraInfoFormat extra information regarding the assertion. Generally a set of name/value
   *     pairs: "id={} fileName={}"
   * @param extraInfoArgs extra information arguments
   * @return the non-null reference that was validated
   * @throws RuntimeException if `reference` is null
   */
  <T> T checkNotNull(
      final T reference,
      final String signature,
      final String extraInfoFormat,
      final Object... extraInfoArgs);

  /**
   * Create a fatal incident if expression is false
   *
   * @param expression condition to test for
   * @param signature signature, small unique string to identify this assertion within the method,
   *     like "path_cannot_be_null"
   * @throws RuntimeException if `condition` is not true
   */
  void check(final boolean expression, final String signature);

  /**
   * Create a fatal incident if expression is false
   *
   * @param expression condition to test for
   * @param signature signature, small unique string to identify this assertion within the method,
   *     like "path_cannot_be_null"
   * @param extraInfoFormat extra information regarding the incident. Generally a set of name/value
   *     pairs: "fileId={} accountId={} fileName={}"
   * @param extraInfoArgs extra information arguments
   * @throws RuntimeException if `condition` is not true
   */
  void check(
      final boolean expression,
      final String signature,
      final String extraInfoFormat,
      final Object... extraInfoArgs);
}
