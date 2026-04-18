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
package org.apache.polaris.persistence.nosql.api.exceptions;

/** Thrown when offset resolution would exceed the configured traversal safeguard. */
public class CommitOffsetTraversalLimitExceededException extends PersistenceException {
  private final String refName;
  private final long offset;
  private final long traversed;
  private final long maxTraversal;

  public CommitOffsetTraversalLimitExceededException(
      String refName, long offset, long traversed, long maxTraversal) {
    super(
        "Exceeded commit offset traversal limit while resolving offset "
            + offset
            + " for ref '"
            + refName
            + "'. Traversed="
            + traversed
            + ", max="
            + maxTraversal);
    this.refName = refName;
    this.offset = offset;
    this.traversed = traversed;
    this.maxTraversal = maxTraversal;
  }

  public String refName() {
    return refName;
  }

  public long offset() {
    return offset;
  }

  public long traversed() {
    return traversed;
  }

  public long maxTraversal() {
    return maxTraversal;
  }
}

