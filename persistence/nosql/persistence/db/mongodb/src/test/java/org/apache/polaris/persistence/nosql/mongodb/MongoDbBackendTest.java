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
package org.apache.polaris.persistence.nosql.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.MongoSocketWriteTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcernResult;
import com.mongodb.bulk.WriteConcernError;
import java.util.List;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.exceptions.UnknownOperationResultException;
import org.bson.BsonDocument;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class MongoDbBackendTest {

  @ParameterizedTest
  @MethodSource("ambiguousWriteExceptions")
  void unhandledExceptionMapsAmbiguousWriteException(RuntimeException exception) {
    var mapped = MongoDbBackend.unhandledException(exception);

    assertThat(mapped).isInstanceOf(UnknownOperationResultException.class);
    assertThat(mapped.getCause()).isSameAs(exception);
  }

  private static Stream<RuntimeException> ambiguousWriteExceptions() {
    var serverAddress = new ServerAddress();
    return Stream.of(
        new MongoSocketClosedException("socket closed", serverAddress),
        new MongoSocketReadException("read failed", serverAddress),
        new MongoSocketWriteException("write failed", serverAddress, null),
        new MongoSocketWriteTimeoutException("write timed out", serverAddress, null),
        new MongoWriteConcernException(
            new WriteConcernError(
                0, "WriteConcernFailed", "write concern failed", new BsonDocument()),
            WriteConcernResult.acknowledged(1, false, null),
            serverAddress,
            List.of()));
  }
}
