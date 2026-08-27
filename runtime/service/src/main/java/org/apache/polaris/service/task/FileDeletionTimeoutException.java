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
package org.apache.polaris.service.task;

/**
 * Signals that a file-cleanup task exceeded its configured deletion timeout. This is treated as a
 * terminal failure for the current execution: the task is not retried in-process, because an
 * immediate retry would only pile a fresh generation of deletions onto an already-stalled endpoint
 * (the previous deletions keep running and cannot be interrupted). The task entity is left in place
 * for the lease-based recovery path rather than being dropped.
 */
class FileDeletionTimeoutException extends RuntimeException {
  FileDeletionTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
