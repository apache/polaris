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

package org.apache.polaris.service.it.env;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.polaris.core.admin.model.Catalog;

/**
 * Annotation to configure the catalog type and properties for integration tests.
 *
 * <p>This is a server-side setting; it is used to specify the Polaris Catalog type (e.g., INTERNAL,
 * EXTERNAL) and any additional properties required for the catalog configuration.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Inherited
public @interface CatalogConfig {

  /** The type of the catalog. Defaults to INTERNAL. */
  Catalog.TypeEnum value() default Catalog.TypeEnum.INTERNAL;

  /** Additional properties for the catalog configuration. */
  String[] properties() default {};
}
