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

import org.apache.polaris.service.types.CreateGenericTableRequest;
import org.apache.polaris.service.types.GenericTable;

public class CatalogGenericTableServiceEvents {
    public record BeforeCreateGenericTableEvent(String prefix, String namespace, CreateGenericTableRequest request) {}
    public record AfterCreateGenericTableEvent(GenericTable table) {}

    public record BeforeDropGenericTableEvent(String prefix, String namespace, String tableName) {}
    public record AfterDropGenericTableEvent(String prefix, String namespace, String tableName) {}

    public record BeforeListGenericTablesEvent(String prefix, String namespace) {}
    public record AfterListGenericTablesEvent(String prefix, String namespace) {}

    public record BeforeLoadGenericTableEvent(String prefix, String namespace, String tableName) {}
    public record AfterLoadGenericTableEvent(GenericTable table) {}
}
