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
package org.apache.polaris.immutables;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import org.immutables.value.Value;

/**
 * A <a href="http://immutables.github.io/style.html#custom-immutable-annotation">Custom
 * {@code @Value.Immutable}</a> with a project-wide common style.
 */
@Documented
@Target(ElementType.TYPE)
@Value.Style(
    defaults = @Value.Immutable(lazyhash = true),
    forceJacksonPropertyNames = false,
    clearBuilder = true,
    depluralize = true,
    toBuilder = "toBuilder",
    get = {"get*", "is*"})
public @interface PolarisImmutable {}
