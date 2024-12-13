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
package org.apache.polaris.core.persistence.resolver;

import org.apache.polaris.core.entity.PolarisEntityType;

public abstract class ResolverException extends RuntimeException {
  // TODO Unify/cleanup this exception w/ `EntityNotFoundException`

  protected ResolverException() {
    super();
  }

  protected ResolverException(String message) {
    super(message);
  }

  public static final class CallerPrincipalNotFoundException extends ResolverException {}

  public static final class PathNotFullyResolvedException extends ResolverException {
    private final ResolverPath failedToResolvePath;
    private final int failedToResolvedEntityIndex;

    public PathNotFullyResolvedException(
        ResolverPath failedToResolvePath, int failedToResolvedEntityIndex) {
      this.failedToResolvePath = failedToResolvePath;
      this.failedToResolvedEntityIndex = failedToResolvedEntityIndex;
    }

    public ResolverPath failedToResolvePath() {
      return failedToResolvePath;
    }

    public int failedToResolvedEntityIndex() {
      return failedToResolvedEntityIndex;
    }
  }

  public static final class EntityNotResolvedException extends ResolverException {
    private final PolarisEntityType failedToResolvedEntityType;
    private final String failedToResolvedEntityName;

    public EntityNotResolvedException(
        PolarisEntityType failedToResolvedEntityType, String failedToResolvedEntityName) {
      this.failedToResolvedEntityType = failedToResolvedEntityType;
      this.failedToResolvedEntityName = failedToResolvedEntityName;
    }

    public PolarisEntityType failedToResolvedEntityType() {
      return failedToResolvedEntityType;
    }

    public String failedToResolvedEntityName() {
      return failedToResolvedEntityName;
    }
  }
}
