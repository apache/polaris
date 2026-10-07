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
package org.apache.polaris.persistence.nosql.dynamodb;

import static java.util.Collections.singletonMap;
import static org.apache.polaris.persistence.nosql.dynamodb.DynamoDbConstants.BATCH_WRITE_LIMIT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

final class BatchWrite implements AutoCloseable {
  private final DynamoDbBackend backend;
  private final String tableName;
  private final List<WriteRequest> requestItems = new ArrayList<>();

  BatchWrite(DynamoDbBackend backend, String tableName) {
    this.backend = backend;
    this.tableName = tableName;
  }

  private void addRequest(WriteRequest.Builder request) {
    requestItems.add(request.build());
    if (requestItems.size() == BATCH_WRITE_LIMIT) {
      flush();
    }
  }

  void addDelete(Map<String, AttributeValue> key) {
    addRequest(WriteRequest.builder().deleteRequest(b -> b.key(key)));
  }

  // close() is actually a flush, implementing AutoCloseable for easier use of BatchDelete using
  // try-with-resources.
  @Override
  public void close() {
    if (!requestItems.isEmpty()) {
      flush();
    }
  }

  private void flush() {
    backend.client.batchWriteItem(b -> b.requestItems(singletonMap(tableName, requestItems)));
    requestItems.clear();
  }
}
