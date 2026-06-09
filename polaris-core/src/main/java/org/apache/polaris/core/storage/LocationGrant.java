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
package org.apache.polaris.core.storage;

import java.util.Set;
import org.jspecify.annotations.NonNull;

/**
 * A request for a specific set of {@link PolarisStorageActions} on a specific set of storage
 * locations. A credential-vending request consists of a list of these grants, allowing different
 * actions to be requested on different prefixes within a single call (e.g. read-only access to a
 * legacy location alongside read+write access to a new location).
 */
public record LocationGrant(
    @NonNull Set<String> locations, @NonNull Set<PolarisStorageActions> actions) {}
