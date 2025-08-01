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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

/** Unit tests for exception mappers */
public class ExceptionMapperTest {
  private static final String MESSAGE = "this is the exception message";
  private static final String CAUSE = "this is the exception cause";

  @ParameterizedTest
  @MethodSource("testFullExceptionIsLogged")
  public void testFullExceptionIsLogged(
      ExceptionMapper<Exception> mapper, Exception exception, Level level) {
    Logger logger = (Logger) LoggerFactory.getLogger(mapper.getClass());
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.start();
    logger.addAppender(listAppender);

    mapper.toResponse(exception);

    Assertions.assertThat(
            listAppender.list.stream()
                .anyMatch(
                    log -> {
                      if (log.getLevel() != level) {
                        return false;
                      }

                      IThrowableProxy proxy = log.getThrowableProxy();
                      if (proxy == null) {
                        return false;
                      }

                      return proxy.getMessage().contains(CAUSE)
                          || Optional.ofNullable(proxy.getCause())
                              .map(IThrowableProxy::getMessage)
                              .orElse("")
                              .contains(CAUSE);
                    }))
        .as("The exception cause should be logged")
        .isTrue();
  }

  @Test
  public void testNamespaceException() {
    PolarisExceptionMapper mapper = new PolarisExceptionMapper();
    Response response = mapper.toResponse(new CommitConflictException("test"));
    Assertions.assertThat(response.getStatus()).isEqualTo(409);
  }

  static Stream<Arguments> testFullExceptionIsLogged() {
    // ConstraintViolationException isn't included because it doesn't propagate any info to its
    // inherited Exception
    return Stream.of(
        Arguments.of(
            new IcebergExceptionMapper(),
            new RuntimeException(MESSAGE, new RuntimeException(CAUSE)),
            Level.ERROR),
        Arguments.of(
            new IcebergJsonProcessingExceptionMapper(),
            new TestJsonProcessingException(MESSAGE, null, new RuntimeException(CAUSE)),
            Level.DEBUG),
        Arguments.of(
            new PolarisExceptionMapper(),
            new AlreadyExistsException(MESSAGE, new RuntimeException(CAUSE)),
            Level.DEBUG));
  }

  private static class TestJsonProcessingException extends JsonProcessingException {
    protected TestJsonProcessingException(String msg, JsonLocation loc, Throwable rootCause) {
      super(msg, loc, rootCause);
    }
  }
}
