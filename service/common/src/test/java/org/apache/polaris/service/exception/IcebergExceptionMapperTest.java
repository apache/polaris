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
package org.apache.polaris.service.exception;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.azure.core.exception.AzureException;
import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.HttpResponse;
import com.google.cloud.storage.StorageException;
import jakarta.ws.rs.core.Response;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class IcebergExceptionMapperTest {

  static Stream<Arguments> fileIOExceptionMapping() {
    Map<Integer, Integer> cloudCodeMappings =
        Map.of(
            // Map of HTTP code returned from a cloud provider to the HTTP code Polaris is expected
            // to return
            302, 422,
            400, 400,
            401, 403,
            403, 403,
            404, 400,
            408, 408,
            429, 429,
            503, 502,
            504, 504);

    return Stream.concat(
        Stream.of(
            Arguments.of(new AzureException("Unknown"), 500),
            Arguments.of(new AzureException("Forbidden"), 403),
            Arguments.of(new AzureException("FORBIDDEN"), 403),
            Arguments.of(new AzureException("Not Authorized"), 403),
            Arguments.of(new AzureException("Access Denied"), 403),
            Arguments.of(S3Exception.builder().message("Access denied").build(), 403),
            Arguments.of(new StorageException(1, "access denied"), 403)),
        cloudCodeMappings.entrySet().stream()
            .flatMap(
                entry ->
                    Stream.of(
                        Arguments.of(
                            new HttpResponseException("", mockAzureResponse(entry.getKey()), ""),
                            entry.getValue()),
                        Arguments.of(
                            S3Exception.builder().message("").statusCode(entry.getKey()).build(),
                            entry.getValue()),
                        Arguments.of(new StorageException(entry.getKey(), ""), entry.getValue()))));
  }

  @ParameterizedTest
  @MethodSource
  void fileIOExceptionMapping(RuntimeException ex, int statusCode) {
    IcebergExceptionMapper mapper = new IcebergExceptionMapper();
    try (Response response = mapper.toResponse(ex)) {
      assertThat(response.getStatus()).isEqualTo(statusCode);
      assertThat(response.getEntity()).extracting("message").isEqualTo(ex.getMessage());
    }
  }

  @Test
  public void testFullExceptionIsLogged() {
    Logger logger = (Logger) LoggerFactory.getLogger(IcebergExceptionMapper.class);
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.start();
    logger.addAppender(listAppender);

    String cause = "this is the exception cause";
    RuntimeException exception = new RuntimeException("message", new RuntimeException(cause));
    IcebergExceptionMapper mapper = new IcebergExceptionMapper();
    mapper.toResponse(exception);

    Assertions.assertThat(
                    listAppender.list.stream()
                            .anyMatch(
                                    log ->
                                            log.getMessage().contains("Full runtimeException")
                                                    && log.getLevel() == Level.DEBUG
                                                    && Optional.ofNullable(log.getThrowableProxy()).map(proxy -> proxy.getCause().getMessage()).orElse("").contains(cause)))
            .as("The exception cause should be logged")
            .isTrue();
  }

  /**
   * Creates a mock of the Azure-specific HttpResponse object, as it's quite difficult to construct
   * a "real" one.
   *
   * @param statusCode
   * @return
   */
  private static HttpResponse mockAzureResponse(int statusCode) {
    HttpResponse res = mock(HttpResponse.class);
    when(res.getStatusCode()).thenReturn(statusCode);
    return res;
  }
}
