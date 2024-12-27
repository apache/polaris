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
package org.apache.polaris.service.context;

import jakarta.validation.constraints.Size;
import java.util.Set;

public interface RealmContextConfiguration {

  /**
   * The set of realms that are supported by the realm context resolver. The first realm is
   * considered the default realm.
   */
  @Size(min = 1)
  Set<String> realms();

  /** The header name that contains the realm identifier. */
  String headerName();

  /** The default realm to use when no realm is specified. */
  default String defaultRealm() {
    return realms().iterator().next();
  }
}
