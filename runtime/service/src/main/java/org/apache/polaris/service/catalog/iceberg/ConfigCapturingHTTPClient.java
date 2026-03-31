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
package org.apache.polaris.service.catalog.iceberg;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A delegating wrapper around the Iceberg {@link RESTClient} that intercepts responses to extract
 * the {@code config} section from loadTable responses. When a {@link LoadTableResponse} is
 * received, the config map (containing {@code tableId} for S3 Tables) is captured and stored in the
 * request-scoped {@link CapturedConfigHolder}.
 */
public class ConfigCapturingHTTPClient implements RESTClient {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigCapturingHTTPClient.class);

  private final RESTClient delegate;
  private final CapturedConfigHolder capturedConfigHolder;

  public ConfigCapturingHTTPClient(RESTClient delegate, CapturedConfigHolder holder) {
    this.delegate = delegate;
    this.capturedConfigHolder = holder;
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    T response = delegate.get(path, queryParams, responseType, headers, errorHandler);
    captureConfigIfLoadTable(response);
    return response;
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    T response = delegate.post(path, body, responseType, headers, errorHandler);
    captureConfigIfLoadTable(response);
    return response;
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delegate.delete(path, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delegate.postForm(path, formData, responseType, headers, errorHandler);
  }

  @Override
  public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
    delegate.head(path, headers, errorHandler);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public RESTClient withAuthSession(org.apache.iceberg.rest.auth.AuthSession session) {
    return new ConfigCapturingHTTPClient(delegate.withAuthSession(session), capturedConfigHolder);
  }

  private <T> void captureConfigIfLoadTable(T response) {
    try {
      if (response instanceof LoadTableResponse loadTableResponse) {
        Map<String, String> config = loadTableResponse.config();
        if (config != null && !config.isEmpty()) {
          capturedConfigHolder.setCapturedConfig(config);
        }
      }
    } catch (Exception e) {
      LOG.warn("Failed to capture config from loadTable response", e);
    }
  }
}
