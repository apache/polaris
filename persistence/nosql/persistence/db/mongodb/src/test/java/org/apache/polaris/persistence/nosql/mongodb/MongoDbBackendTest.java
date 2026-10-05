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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.MongoSocketWriteTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcernResult;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteConcernError;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.exceptions.UnknownOperationResultException;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
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
            List.of()),
        bulkWriteConcernException());
  }

  @Test
  void unhandledExceptionKeepsBulkDuplicateKey() {
    var exception =
        new MongoBulkWriteException(
            BulkWriteResult.acknowledged(0, 0, 0, 0, List.of(), List.of()),
            List.of(new BulkWriteError(11000, "duplicate key", new BsonDocument(), 0)),
            null,
            new ServerAddress(),
            Set.of());

    assertThat(MongoDbBackend.unhandledException(exception)).isSameAs(exception);
  }

  @Test
  void bulkWriteConcernErrorIsAFailureEvenWithoutWriteErrors() {
    assertThat(MongoDbBackend.isBulkWriteFailure(bulkWriteConcernException())).isTrue();
  }

  @Test
  void bulkDuplicateKeyOnlyIsNotAFailure() {
    var exception =
        new MongoBulkWriteException(
            BulkWriteResult.acknowledged(0, 0, 0, 0, List.of(), List.of()),
            List.of(new BulkWriteError(11000, "duplicate key", new BsonDocument(), 0)),
            null,
            new ServerAddress(),
            Set.of());

    assertThat(MongoDbBackend.isBulkWriteFailure(exception)).isFalse();
  }

  /**
   * A bulk write that failed only on write concern. {@code BulkWriteBatchCombiner} builds this
   * shape with an empty write-error list, because its {@code hasErrors()} is {@code
   * hasWriteErrors() || hasWriteConcernErrors()}.
   */
  private static MongoBulkWriteException bulkWriteConcernException() {
    return new MongoBulkWriteException(
        BulkWriteResult.acknowledged(0, 1, 0, 1, List.of(), List.of()),
        List.of(),
        new WriteConcernError(
            64, "WriteConcernFailed", "waiting for replication timed out", new BsonDocument()),
        new ServerAddress(),
        Set.of());
  }
}
