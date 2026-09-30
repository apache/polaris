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
package org.apache.polaris.persistence.nosql.cassandra;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Qualifies a Cassandra Java-driver authentication provider by its configured name.
 *
 * <p>Applications can provide an {@link AuthProvider} with this qualifier and select it with {@code
 * auth.provider-name}.
 */
@Qualifier
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface CassandraAuthentication {
  /** The configuration name of the authentication provider. */
  String value();

  /** Helper for selecting a named Cassandra authentication provider programmatically. */
  final class Literal extends AnnotationLiteral<CassandraAuthentication>
      implements CassandraAuthentication {
    private final String value;

    public static Literal of(String value) {
      return new Literal(value);
    }

    private Literal(String value) {
      this.value = value;
    }

    @Override
    public String value() {
      return value;
    }
  }
}
