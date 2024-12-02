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
package org.apache.polaris.service.tracing;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import java.net.http.HttpRequest;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * Implementation of {@link TextMapSetter} and {@link TextMapGetter} that can handle an {@link
 * HttpServletRequest} for extracting headers and sets headers on a {@link HttpRequest.Builder}.
 */
public class HeadersMapAccessor
    implements TextMapSetter<HttpRequest.Builder>, TextMapGetter<HttpServletRequest> {
  @Override
  public Iterable<String> keys(HttpServletRequest carrier) {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                carrier.getHeaderNames().asIterator(), Spliterator.IMMUTABLE),
            false)
        .toList();
  }

  @Nullable
  @Override
  public String get(@Nullable HttpServletRequest carrier, String key) {
    return carrier == null ? null : carrier.getHeader(key);
  }

  @Override
  public void set(@Nullable HttpRequest.Builder carrier, String key, String value) {
    if (carrier != null) {
      carrier.setHeader(key, value);
    }
  }
}
