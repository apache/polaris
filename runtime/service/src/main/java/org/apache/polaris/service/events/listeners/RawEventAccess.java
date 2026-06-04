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
package org.apache.polaris.service.events.listeners;

/**
 * Marker interface signalling that a {@link PolarisEventListener} requires the raw, unsanitized
 * event. Listeners that implement this interface receive the original event from {@link
 * org.apache.polaris.service.events.PolarisEventListeners#deliverEvent}, bypassing the global
 * {@link org.apache.polaris.service.events.EventSanitizer}.
 *
 * <p>This is an explicit security opt-out. Implement only when the listener has a documented need
 * for sensitive attributes (for example, a forensic audit sink that records principal credentials
 * or raw catalog configuration). All other listeners receive the sanitized event by default.
 */
public interface RawEventAccess {}
