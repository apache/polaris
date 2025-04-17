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

import org.apache.polaris.core.context.CallContext;

/**
 * Emitted after an attempt of an async task, such as manifest file cleanup, finishes.
 *
 * @param taskEntityId The ID of the TaskEntity.
 * @param callContext The CallContext the task is being executed under.
 * @param attempt The attempt number. Each retry of the task will have its own attempt number. The
 *     initial (non-retried) attempt starts counting from 1.
 * @param success Whether or not the attempt succeeded.
 */
public record AfterTaskAttemptedEvent(
    long taskEntityId, CallContext callContext, int attempt, boolean success)
    implements PolarisEvent {}
