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

package org.apache.polaris.service.events;

/**
 * Per-attribute denial policy used by the default {@link EventSanitizer} implementation. Decides
 * whether a given {@link AttributeKey} is safe to pass downstream. This is a sub-component of
 * {@link EventSanitizer}, not a standalone global hook: replacing only this bean tweaks which
 * attributes are denied while keeping the standard derivation logic intact. To change derivation or
 * add raw-event passthrough, replace {@link EventSanitizer} instead.
 */
public interface EventAttributeFilter {

  boolean isAllowed(AttributeKey<?> key);
}
