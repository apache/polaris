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

import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.PersistenceException;
import jakarta.ws.rs.core.Response;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;


public class IcebergPersistenceExceptionMapperTest {

  static Stream<PersistenceException> persistenceExceptionMapping() {
    return Stream.of(
        new EntityNotFoundException("Entity not found")
    );
  }

  @ParameterizedTest
  @MethodSource
  void persistenceExceptionMapping(PersistenceException ex) {
    IcebergPersistenceExceptionMapper mapper = new IcebergPersistenceExceptionMapper();

    try (Response response = mapper.toResponse(ex)) {
      assertThat(response.getStatus()).isEqualTo(500);
      assertThat(response.getEntity()).extracting("message").isEqualTo(ex.getMessage());
    }
  }
}
