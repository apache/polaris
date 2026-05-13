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
 * Determines which event attributes are safe to pass downstream for persistence or external
 * consumption. Implementations decide which {@link AttributeKey}s from an {@link EventAttributeMap}
 * should be retained.
 *
 * <p>This interface is intentionally placed at the event pipeline level (not within a specific
 * listener package) so that it can be promoted to a global pre-delivery filter once community
 * consensus is reached on applying sanitization before all listeners, not just persistence.
 */
public interface EventAttributeFilter {

  boolean isAllowed(AttributeKey<?> key);
}
