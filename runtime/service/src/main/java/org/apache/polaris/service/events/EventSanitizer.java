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

import org.apache.polaris.service.events.listeners.RawEventAccess;

/**
 * Global, dispatcher-enforced sanitizer for events. Invoked once per listener delivery from {@link
 * PolarisEventListeners#deliverEvent(PolarisEvent, String,
 * org.apache.polaris.service.events.listeners.PolarisEventListener)
 * PolarisEventListeners.deliverEvent}, before each listener that has not opted in via {@link
 * RawEventAccess}. Each invocation produces a fresh sanitized copy; the original event is never
 * mutated.
 *
 * <p>Implementations are responsible for both stripping disallowed attributes and producing any
 * derived attributes that downstream listeners depend on. Replacing this bean replaces the entire
 * global sanitization policy — attribute denial and derivation.
 */
public interface EventSanitizer {

  /**
   * Returns a sanitized copy of {@code event}. The original event is not modified. The returned
   * event MUST be safe to deliver to all listeners that have not opted in via {@link
   * RawEventAccess}.
   */
  PolarisEvent sanitize(PolarisEvent event);
}
