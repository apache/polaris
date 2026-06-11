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
package org.apache.polaris.service.idempotency;

/**
 * Operations that support handler-level idempotency. The {@link #wireName()} is the stable string
 * persisted in the {@code operation_type} column and compared as part of the idempotency binding;
 * it must not change once released.
 */
public enum IdempotentOperation {
  CREATE_TABLE("create-table");

  private final String wireName;

  IdempotentOperation(String wireName) {
    this.wireName = wireName;
  }

  /** Stable persisted/compared identifier for this operation. */
  public String wireName() {
    return wireName;
  }
}
