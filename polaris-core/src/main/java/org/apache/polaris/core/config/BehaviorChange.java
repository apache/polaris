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
package org.apache.polaris.core.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that should be applied to all BehaviorChangeConfiguration instances. Spotless will
 * enforce that these flags are not around too long. When the flag expires, the linter will prompt
 * the person attempting to do the release to take 1 of 3 actions:
 *
 * <pre>
 * <ul>
 *     <li>Remove the flag</li>
 *     <li>Promote the flag to a feature flag</li>
 *     <li>Disable the linter for this flag for an "extension"</li>
 * </ul>
 * </pre>
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface BehaviorChange {
  static final String AUTOMATIC_EXPIRES = "auto";

  String since();

  String expires() default AUTOMATIC_EXPIRES;
}
