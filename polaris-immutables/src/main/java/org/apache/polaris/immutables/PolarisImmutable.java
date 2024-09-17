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
 * {@code @Value.Immutable}</a> with the following defaults:
 *
 * <ul>
 *   <li><b>lazyhash</b> - Set to {@code true} to generate hash code lazily for performance
 *       benefits.
 *   <li><b>clearBuilder</b> - Set to {@code true} to be able to reuse builder instances.
 *   <li><b>depluralize</b> - Set to {@code true} to use singular names for collections and arrays
 *       in JavaBeans-style accessors and builder methods.
 *   <li><b>get</b> - Accessor prefixes set to {@code get*} and {@code is*} to emulate
 *       JavaBeans-style getters.
 *   <li><b>forceJacksonPropertyNames</b> - Set to {@code false} since we use JavaBeans-style
 *       getters, and for better compatibility with custom naming strategies.
 * </ul>
 */
@Documented
@Target(ElementType.TYPE)
@Value.Style(
    defaults = @Value.Immutable(lazyhash = true),
    clearBuilder = true,
    depluralize = true,
    get = {"get*", "is*"},
    forceJacksonPropertyNames = false)
public @interface PolarisImmutable {}
