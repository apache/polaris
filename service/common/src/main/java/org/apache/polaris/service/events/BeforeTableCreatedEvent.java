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

package org.apache.polaris.service.events;

import org.apache.iceberg.catalog.TableIdentifier;

/** Emitted when Polaris intends to create a table. */
public final class BeforeTableCreatedEvent extends PolarisEvent {
  private final TableIdentifier identifier;
  private final String requestId;

  public BeforeTableCreatedEvent(TableIdentifier tableIdentifier, String requestId) {
    this.identifier = tableIdentifier;
    this.requestId = requestId;
  }

  public TableIdentifier getIdentifier() {
    return identifier;
  }

  public String getRequestId() {
    return requestId;
  }
}
