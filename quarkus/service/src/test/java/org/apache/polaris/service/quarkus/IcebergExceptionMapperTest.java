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
package org.apache.polaris.service.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.core.exception.AzureException;
import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.HttpResponse;
import com.google.cloud.storage.StorageException;
import jakarta.ws.rs.core.Response;
import java.util.stream.Stream;
import org.apache.polaris.service.exception.IcebergExceptionMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.model.S3Exception;

class IcebergExceptionMapperTest {

  static Stream<Arguments> fileIOExceptionMapping() {
    return Stream.of(
        Arguments.of(new AzureException("Unknown"), 500),
        Arguments.of(new AzureException("Forbidden"), 403),
        Arguments.of(new AzureException("FORBIDDEN"), 403),
        Arguments.of(new AzureException("Not Authorized"), 403),
        Arguments.of(new AzureException("Access Denied"), 403),
        Arguments.of(new HttpResponseException("", mockAzureResponse(400), ""), 400),
        Arguments.of(new HttpResponseException("", mockAzureResponse(401), ""), 403),
        Arguments.of(new HttpResponseException("", mockAzureResponse(403), ""), 403),
        Arguments.of(new HttpResponseException("", mockAzureResponse(404), ""), 400),
        Arguments.of(new HttpResponseException("", mockAzureResponse(429), ""), 429),
        Arguments.of(new HttpResponseException("", mockAzureResponse(504), ""), 500),
        Arguments.of(new HttpResponseException("", mockAzureResponse(302), ""), 502),
        Arguments.of(S3Exception.builder().message("Access denied").build(), 403),
        Arguments.of(S3Exception.builder().message("").statusCode(400).build(), 400),
        Arguments.of(S3Exception.builder().message("").statusCode(401).build(), 403),
        Arguments.of(S3Exception.builder().message("").statusCode(403).build(), 403),
        Arguments.of(S3Exception.builder().message("").statusCode(404).build(), 400),
        Arguments.of(S3Exception.builder().message("").statusCode(429).build(), 429),
        Arguments.of(S3Exception.builder().message("").statusCode(504).build(), 500),
        Arguments.of(S3Exception.builder().message("").statusCode(302).build(), 502),
        Arguments.of(new StorageException(1, "access denied"), 403),
        Arguments.of(new StorageException(400, ""), 400),
        Arguments.of(new StorageException(401, ""), 403),
        Arguments.of(new StorageException(403, ""), 403),
        Arguments.of(new StorageException(404, ""), 400),
        Arguments.of(new StorageException(429, ""), 429),
        Arguments.of(new StorageException(504, ""), 500),
        Arguments.of(new StorageException(302, ""), 502));
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
