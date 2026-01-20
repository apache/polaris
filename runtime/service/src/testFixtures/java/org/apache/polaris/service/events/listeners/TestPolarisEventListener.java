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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;

/** Test event listener that stores all emitted events by type. */
public class TestPolarisEventListener implements PolarisEventListener {
  private final Map<PolarisEventType, PolarisEvent> latestEvents = new ConcurrentHashMap<>();

  @Override
  public void onEvent(PolarisEvent event) {
    latestEvents.put(event.type(), event);
  }

  public void clear() {
    latestEvents.clear();
  }

  public PolarisEvent getLatest(PolarisEventType type) {
    var latest = latestEvents.get(type);
    if (latest == null) {
      throw new IllegalStateException("No event of type " + type + " recorded");
    }
    return latest;
  }

}
