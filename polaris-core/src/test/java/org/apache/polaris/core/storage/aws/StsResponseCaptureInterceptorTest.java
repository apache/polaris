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
package org.apache.polaris.core.storage.aws;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;

public class StsResponseCaptureInterceptorTest {

  @BeforeEach
  public void before() {
    StsResponseCapture.clear();
  }

  // Local interface used by the dynamic proxy to provide content()
  @SuppressWarnings("unused")
  private interface ContentHolder {
    java.util.Optional<java.io.InputStream> content();
  }

  @Test
  public void testAfterTransmissionCapturesBody() {
    StsResponseCaptureInterceptor interceptor = new StsResponseCaptureInterceptor();
    try {
      Class<?> afterCls = software.amazon.awssdk.core.interceptor.Context.AfterTransmission.class;
      Class<?> httpRespType = afterCls.getMethod("httpResponse").getReturnType();

      // Build a response proxy that implements both the SDK response return type and ContentHolder
      Object respProxy =
          java.lang.reflect.Proxy.newProxyInstance(
              httpRespType.getClassLoader(),
              new Class[] {httpRespType, ContentHolder.class},
              (proxy, method, args) -> {
                if ("content".equals(method.getName())) {
                  return Optional.of(
                      new ByteArrayInputStream(
                          "raw-body-xyz".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                }
                return null;
              });

      // Now build an AfterTransmission proxy that returns the respProxy from httpResponse()
      Object ctxProxy =
          java.lang.reflect.Proxy.newProxyInstance(
              afterCls.getClassLoader(),
              new Class[] {afterCls},
              (proxy, method, args) -> {
                if ("httpResponse".equals(method.getName())) {
                  return respProxy;
                }
                return null;
              });

      interceptor.afterTransmission(
          (software.amazon.awssdk.core.interceptor.Context.AfterTransmission) ctxProxy,
          new ExecutionAttributes());
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
    // Because we cast via reflection, ensure the captured body is set
    assertThat(StsResponseCapture.getLastBody()).isEqualTo("raw-body-xyz");
  }

  @Test
  public void testAfterTransmissionSilentlyIgnoresUnknownContext() {
    StsResponseCaptureInterceptor interceptor = new StsResponseCaptureInterceptor();
    // Create a mock AfterTransmission that returns null for httpResponse()
    software.amazon.awssdk.core.interceptor.Context.AfterTransmission nullRespContext =
        org.mockito.Mockito.mock(
            software.amazon.awssdk.core.interceptor.Context.AfterTransmission.class);
    org.mockito.Mockito.when(nullRespContext.httpResponse()).thenReturn(null);
    interceptor.afterTransmission(nullRespContext, new ExecutionAttributes());
    assertThat(StsResponseCapture.getLastBody()).isNull();
  }
}
