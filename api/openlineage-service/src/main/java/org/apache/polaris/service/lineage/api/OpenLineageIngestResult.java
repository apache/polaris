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
package org.apache.polaris.service.lineage.api;

/**
 * Outcome returned by {@link OpenLineageIngestProvider}. The adapter maps this to the appropriate
 * HTTP status; provider implementations do not construct JAX-RS responses.
 */
public enum OpenLineageIngestResult {
  /** Event was accepted. Maps to {@code 201 Created}. */
  ACCEPTED,

  /** Event was rejected (e.g. validation failure). Maps to {@code 400 Bad Request}. */
  REJECTED,

  /** Ingest backend is not available. Maps to {@code 503 Service Unavailable}. */
  UNAVAILABLE
}
