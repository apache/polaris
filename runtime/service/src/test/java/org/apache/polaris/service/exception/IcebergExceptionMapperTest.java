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

import com.azure.core.exception.AzureException;
import com.azure.core.exception.HttpResponseException;
import com.google.cloud.storage.StorageException;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.polaris.core.exceptions.FileIOUnknownHostException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class IcebergExceptionMapperTest {

  static Stream<Arguments> fileIOExceptionMapping() {
    Map<Integer, Integer> cloudCodeMappings =
        Map.of(
            // Map of HTTP code returned from a cloud provider to the HTTP code Polaris is expected
            // to return. We create a test case for each of these mappings for each cloud provider.
            // We also create a test case for cloud provider exceptions wrapped as
            // RuntimeIOExceptions,
            // which is what the Iceberg SDK sometimes wraps them with if the error happens during
            // IO.
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
            Arguments.of(new StorageException(1, "access denied"), 403),
            Arguments.of(
                new FileIOUnknownHostException(
                    "mybucket.blob.core.windows.net: Name or service not known",
                    new RuntimeException(new UnknownHostException())),
                404),
            Arguments.of(new RuntimeException("Error persisting entity"), 500)),
        cloudCodeMappings.entrySet().stream()
            .flatMap(
                entry ->
                    Stream.of(
                            Arguments.of(
                                new HttpResponseException(
                                    "", new FakeAzureHttpResponse(entry.getKey()), ""),
                                entry.getValue()),
                            Arguments.of(
                                S3Exception.builder()
                                    .message("")
                                    .statusCode(entry.getKey())
                                    .build(),
                                entry.getValue()),
                            Arguments.of(
                                new StorageException(entry.getKey(), ""), entry.getValue()))
                        .flatMap(
                            args ->
                                Stream.of(
                                    args,
                                    Arguments.of(
                                        new RuntimeIOException(
                                            new IOException((Throwable) args.get()[0])),
                                        args.get()[1])))));
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
}
