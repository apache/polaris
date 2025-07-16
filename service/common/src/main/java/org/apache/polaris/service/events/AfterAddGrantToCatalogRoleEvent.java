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

import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.entity.PolarisPrivilege;

/**
 * Event fired after a grant is added to a catalog role in Polaris.
 *
 * @param eventId the unique identifier for this event
 * @param catalogName the name of the catalog
 * @param catalogRoleName the name of the catalog role
 * @param privilege the privilege granted
 * @param grantRequest the grant request
 */
public record AfterAddGrantToCatalogRoleEvent(String eventId, String catalogName, String catalogRoleName, PolarisPrivilege privilege, AddGrantRequest grantRequest) implements PolarisEvent {}
