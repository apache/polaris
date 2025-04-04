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
package org.apache.polaris.extension.persistence.relational.jdbc;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import java.util.Optional;

@StaticInitSafe
@ConfigMapping(prefix = "polaris.persistence.relational")
public interface RelationalJdbcConfiguration {
  Optional<String> jdbcUrl();

  Optional<String> userName();

  Optional<String> password();

  Optional<String> driverClassName();

  Optional<Integer> initialPoolSize();

  Optional<Integer> maxTotal();

  Optional<Integer> maxIdle();

  Optional<Integer> minIdle();

  Optional<Long> maxWaitMillis();

  Optional<Boolean> testOnBorrow();

  Optional<Boolean> testOnReturn();

  Optional<Boolean> testWhileIdle();

  Optional<String> validationQuery();

  Optional<Long> timeBetweenEvictionRunsMillis();

  Optional<Integer> minEvictableIdleTimeMillis();
}
