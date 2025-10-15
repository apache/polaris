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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import software.amazon.awssdk.core.interceptor.Context.AfterTransmission;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

/**
 * ExecutionInterceptor that captures the raw HTTP response body for STS calls and saves it into
 * StsResponseCapture (thread-local). This allows calling code to inspect the raw response when the
 * SDK's unmarshalling yields null credentials.
 */
public class StsResponseCaptureInterceptor implements ExecutionInterceptor {

  @Override
  public void afterTransmission(
      AfterTransmission context, ExecutionAttributes executionAttributes) {
    try {
      // Use reflection to call context.httpResponse() because SDK versions expose different
      // types/APIs
      Method httpRespMethod = context.getClass().getMethod("httpResponse");
      Object httpResp = httpRespMethod.invoke(context);
      if (httpResp != null) {
        try {
          Optional<InputStream> content = OptionalUtils.safeGetContent(httpResp);
          if (content.isPresent()) {
            try (InputStream in = content.get();
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
              byte[] buf = new byte[8192];
              int r;
              while ((r = in.read(buf)) != -1) {
                out.write(buf, 0, r);
              }
              String resp = new String(out.toByteArray(), StandardCharsets.UTF_8);
              StsResponseCapture.setLastBody(resp);
            }
          }
        } catch (Exception e) {
          // best-effort; don't fail the call because of capture problems
        }
      }
    } catch (Throwable t) {
      // swallow - capture is non-fatal
    }
  }
}

// Small utility to safely read content from SdkHttpResponse across SDK versions.
final class OptionalUtils {
  static Optional<InputStream> safeGetContent(Object httpResp) {
    try {
      Method contentMethod = httpResp.getClass().getMethod("content");
      Object val = contentMethod.invoke(httpResp);
      if (val == null) return Optional.empty();
      if (val instanceof Optional) {
        Optional<?> anyOpt = (Optional<?>) val;
        if (anyOpt.isPresent() && anyOpt.get() instanceof InputStream) {
          return Optional.of((InputStream) anyOpt.get());
        }
        return Optional.empty();
      }
      // Some SDKs may return an InputStream directly
      if (val instanceof InputStream) {
        return Optional.of((InputStream) val);
      }
    } catch (NoSuchMethodException nsme) {
      // ignore
    } catch (Exception e) {
      // ignore other reflection errors
    }
    return Optional.empty();
  }
}
