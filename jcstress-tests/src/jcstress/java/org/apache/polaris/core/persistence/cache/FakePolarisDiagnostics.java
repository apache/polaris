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
package org.apache.polaris.core.persistence.cache;

import org.apache.polaris.core.PolarisDiagnostics;

/**
 * Fake implementation of {@link PolarisDiagnostics} for testing purposes. This implementation does
 * nothing. Any actual use of this class will likely throw a {@link NullPointerException} as all
 * methods return null.
 */
public class FakePolarisDiagnostics implements PolarisDiagnostics {
  @Override
  public RuntimeException fail(String signature, String extraInfoFormat, Object... extraInfoArgs) {
    return null;
  }

  @Override
  public RuntimeException fail(
      String signature, Throwable cause, String extraInfoFormat, Object... extraInfoArgs) {
    return null;
  }

  @Override
  public <T> T checkNotNull(T reference, String signature) {
    return null;
  }

  @Override
  public <T> T checkNotNull(
      T reference, String signature, String extraInfoFormat, Object... extraInfoArgs) {
    return null;
  }

  @Override
  public void check(boolean expression, String signature) {}

  @Override
  public void check(
      boolean expression, String signature, String extraInfoFormat, Object... extraInfoArgs) {}
}
