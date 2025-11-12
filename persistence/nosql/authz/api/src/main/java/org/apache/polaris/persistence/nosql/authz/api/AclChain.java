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
package org.apache.polaris.persistence.nosql.authz.api;

import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/** Container for an {@linkplain Acl ACL} of an individual entity and a pointer to its parent. */
@PolarisImmutable
public interface AclChain {
  @Value.Parameter(order = 1)
  Acl acl();

  @Value.Parameter(order = 2)
  Optional<AclChain> parent();

  static AclChain aclChain(Acl acl, Optional<AclChain> parent) {
    return ImmutableAclChain.of(acl, parent);
  }
}
