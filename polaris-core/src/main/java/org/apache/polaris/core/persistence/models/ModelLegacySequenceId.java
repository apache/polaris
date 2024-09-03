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
package org.apache.polaris.core.persistence.models;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

@Entity
@Table(
    name = "POLARIS_SEQ",
    indexes = {@Index(name = "idx_last_value", columnList = "last_value")})
/**
 * Used to maintain backwards compatibility with previous versions of Polaris that used a generator
 * POLARIS_SEQ to generate IDs.
 *
 * <p>This shouldn't be written to, and is replced by `ModelSequenceId` going forward.
 */
public class ModelLegacySequenceId {
  @Id
  @Column(name = "last_value", nullable = false)
  private Long id;

  public void setId(Long id) {
    this.id = id;
  }

  public Long getId() {
    return id;
  }
}