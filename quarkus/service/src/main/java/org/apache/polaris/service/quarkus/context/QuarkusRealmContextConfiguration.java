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
package org.apache.polaris.service.quarkus.context;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import org.apache.polaris.service.context.RealmContextConfiguration;

@StaticInitSafe
@ConfigMapping(prefix = "polaris.realm-context")
public interface QuarkusRealmContextConfiguration extends RealmContextConfiguration {

  /**
   * The type of the realm context resolver. Must be a registered {@link
   * org.apache.polaris.service.context.RealmContextResolver} identifier.
   */
  String type();
}
