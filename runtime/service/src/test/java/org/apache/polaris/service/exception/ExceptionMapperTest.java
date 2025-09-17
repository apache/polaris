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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.jboss.logmanager.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.impl.Slf4jLogger;

/** Unit tests for exception mappers */
@SuppressWarnings("resource")
public class ExceptionMapperTest {
  private static final String MESSAGE = "this is the exception message";
  private static final String CAUSE = "this is the exception cause";

  private static final org.jboss.logmanager.Logger JBOSS_LOGGER =
      Mockito.mock(org.jboss.logmanager.Logger.class);

  @BeforeEach
  void setUp() {
    reset(JBOSS_LOGGER);
    when(JBOSS_LOGGER.isLoggable(any())).thenReturn(true);
  }

  @ParameterizedTest
  @MethodSource("testFullExceptionIsLogged")
  public void testFullExceptionIsLogged(
      ExceptionMapper<Exception> mapper, Exception exception, Level level) {

    mapper.toResponse(exception);

    verify(JBOSS_LOGGER)
        .logRaw(
            assertArg(
                record -> {
                  assertThat(record.getLevel()).isEqualTo(level);

                  String message = record.getMessage();
                  if (message == null) {
                    return;
                  }

                  Throwable error = record.getThrown();
                  if (error == null) {
                    return;
                  }

                  assertThat(
                          Optional.ofNullable(error.getCause())
                              .map(Throwable::getMessage)
                              .orElse(message))
                      .contains(CAUSE);
                }));
  }

  @Test
  public void testNamespaceException() {
    PolarisExceptionMapper mapper = new PolarisExceptionMapper();
    Response response = mapper.toResponse(new CommitConflictException("test"));
    assertThat(response.getStatus()).isEqualTo(409);
  }

  static Stream<Arguments> testFullExceptionIsLogged() {
    // ConstraintViolationException isn't included because it doesn't propagate any info to its
    // inherited Exception
    return Stream.of(
        Arguments.of(
            new IcebergExceptionMapper() {
              @Override
              Logger getLoggerForExceptionLogging() {
                return new Slf4jLogger(JBOSS_LOGGER);
              }
            },
            new RuntimeException(MESSAGE, new RuntimeException(CAUSE)),
            Level.ERROR),
        Arguments.of(
            new IcebergExceptionMapper() {
              @Override
              Logger getLoggerForExceptionLogging() {
                return new Slf4jLogger(JBOSS_LOGGER);
              }
            },
            new RuntimeIOException(new IOException(new RuntimeException(CAUSE)), "%s", MESSAGE),
            Level.INFO),
        Arguments.of(
            new IcebergExceptionMapper() {
              @Override
              Logger getLoggerForExceptionLogging() {
                return new Slf4jLogger(JBOSS_LOGGER);
              }
            },
            new ForbiddenException(MESSAGE, new RuntimeException(CAUSE)),
            Level.DEBUG),
        Arguments.of(
            new IcebergJsonProcessingExceptionMapper() {
              @Override
              Logger getLoggerForExceptionLogging() {
                return new Slf4jLogger(JBOSS_LOGGER);
              }
            },
            new TestJsonProcessingException(MESSAGE, null, new RuntimeException(CAUSE)),
            Level.DEBUG),
        Arguments.of(
            new IcebergJsonProcessingExceptionMapper() {
              @Override
              Logger getLoggerForExceptionLogging() {
                return new Slf4jLogger(JBOSS_LOGGER);
              }
            },
            new TestJsonProcessingException(MESSAGE, null, new RuntimeException(CAUSE)),
            Level.DEBUG),
        Arguments.of(
            new IcebergJsonProcessingExceptionMapper() {
              @Override
              Logger getLoggerForExceptionLogging() {
                return new Slf4jLogger(JBOSS_LOGGER);
              }
            },
            new JsonGenerationException(MESSAGE, new RuntimeException(CAUSE), null),
            Level.ERROR),
        Arguments.of(
            new PolarisExceptionMapper() {
              @Override
              Logger getLogger() {
                return new Slf4jLogger(JBOSS_LOGGER);
              }
            },
            new AlreadyExistsException(MESSAGE, new RuntimeException(CAUSE)),
            Level.DEBUG));
  }

  private static class TestJsonProcessingException extends JsonProcessingException {
    protected TestJsonProcessingException(String msg, JsonLocation loc, Throwable rootCause) {
      super(msg, loc, rootCause);
    }
  }
}
