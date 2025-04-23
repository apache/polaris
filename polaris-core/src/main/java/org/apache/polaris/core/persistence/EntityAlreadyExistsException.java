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
package org.apache.polaris.core.persistence;

import org.apache.polaris.core.entity.PolarisBaseEntity;

/**
 * Exception raised when some kind of conclit in the persistence layer prevents the attempted
 * creation of a new entity; provides a member holding the conflicting entity.
 */
public class EntityAlreadyExistsException extends RuntimeException {
  private final PolarisBaseEntity existingEntity;

  /**
   * @param existingEntity The conflicting entity that caused creation to fail.
   */
  public EntityAlreadyExistsException(PolarisBaseEntity existingEntity) {
    super(message(existingEntity));
    this.existingEntity = existingEntity;
  }

  public EntityAlreadyExistsException(PolarisBaseEntity existingEntity, Throwable cause) {
    super(message(existingEntity), cause);
    this.existingEntity = existingEntity;
  }

  private static String message(PolarisBaseEntity existingEntity) {
    return existingEntity.getName() + ":" + existingEntity.getId();
  }

  public PolarisBaseEntity getExistingEntity() {
    return this.existingEntity;
  }
}
