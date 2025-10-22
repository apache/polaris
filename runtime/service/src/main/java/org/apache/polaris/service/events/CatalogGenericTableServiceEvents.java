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

import java.util.UUID;
import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.GenericTable;

public class CatalogGenericTableServiceEvents {
  public record BeforeCreateGenericTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      CreateGenericTableRequest request)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_CREATE_GENERIC_TABLE;
    }
  }

  public record AfterCreateGenericTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      GenericTable table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_CREATE_GENERIC_TABLE;
    }
  }

  public record BeforeDropGenericTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String tableName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_DROP_GENERIC_TABLE;
    }
  }

  public record AfterDropGenericTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String tableName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_DROP_GENERIC_TABLE;
    }
  }

  public record BeforeListGenericTablesEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LIST_GENERIC_TABLES;
    }
  }

  public record AfterListGenericTablesEvent(
      UUID id, PolarisEventMetadata metadata, String catalogName, String namespace)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LIST_GENERIC_TABLES;
    }
  }

  public record BeforeLoadGenericTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      String tableName)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.BEFORE_LOAD_GENERIC_TABLE;
    }
  }

  public record AfterLoadGenericTableEvent(
      UUID id,
      PolarisEventMetadata metadata,
      String catalogName,
      String namespace,
      GenericTable table)
      implements PolarisEvent {
    @Override
    public PolarisEventType type() {
      return PolarisEventType.AFTER_LOAD_GENERIC_TABLE;
    }
  }
}
