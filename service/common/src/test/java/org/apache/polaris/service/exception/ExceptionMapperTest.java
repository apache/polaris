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
import jakarta.ws.rs.ext.ExceptionMapper;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

public class ExceptionMapperTest {
  private static final String CAUSE = "this is the exception cause";

  @ParameterizedTest
  @MethodSource("testFullExceptionIsLogged")
  public void testFullExceptionIsLogged(ExceptionMapper mapper, Exception exception) {
    Logger logger = (Logger) LoggerFactory.getLogger(mapper.getClass());
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.start();
    logger.addAppender(listAppender);

    mapper.toResponse(exception);

    Assertions.assertThat(
            listAppender.list.stream()
                .anyMatch(
                    log -> {
                      if (log.getLevel() != Level.DEBUG) {
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

  static Stream<Arguments> testFullExceptionIsLogged() {
    // ConstraintViolationException isn't included because it doesn't propagate any info to its
    // inherited Exception
    return Stream.of(
        Arguments.of(
            new IcebergExceptionMapper(),
            new RuntimeException("message", new RuntimeException(CAUSE))),
        Arguments.of(
            new IcebergJsonProcessingExceptionMapper(),
            new TestJsonProcessingException("message", null, new RuntimeException(CAUSE))),
        Arguments.of(
            new PolarisExceptionMapper(),
            new AlreadyExistsException("message", new RuntimeException(CAUSE))));
  }

  private static class TestJsonProcessingException extends JsonProcessingException {
    protected TestJsonProcessingException(String msg, JsonLocation loc, Throwable rootCause) {
      super(msg, loc, rootCause);
    }
  }
}
