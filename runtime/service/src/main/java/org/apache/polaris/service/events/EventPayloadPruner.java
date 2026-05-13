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

import java.util.Map;

/**
 * Reduces large attribute values to bounded, safe representations suitable for persistence or
 * external transmission. While {@link EventAttributeFilter} decides <em>which</em> keys pass, this
 * interface decides <em>how much</em> of a passing value is retained.
 *
 * <p>This interface is intentionally placed at the event pipeline level so that it can be promoted
 * to a global pre-delivery pruner once community consensus is reached on applying payload
 * sanitization before all listeners, not just persistence.
 */
public interface EventPayloadPruner {

  Map<String, String> prune(AttributeKey<?> key, Object value);
}
