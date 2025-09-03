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

package org.apache.polaris.service.events.jsonEventListener;

import java.util.HashMap;
import org.apache.polaris.service.events.AfterTableRefreshedEvent;
import org.apache.polaris.service.events.listeners.PolarisEventListener;

/**
 * This class provides a common framework for transforming Polaris events into a HashMap, which can
 * be used to transform the event further, such as transforming into a JSON string, and send them to
 * various destinations. Concrete implementations should override the
 * {{@code @link#transformAndSendEvent(HashMap)}} method to define how the event data should be
 * transformed into a JSON string, transmitted, and/or stored.
 */
public abstract class PropertyMapEventListener extends PolarisEventListener {
  protected abstract void transformAndSendEvent(HashMap<String, Object> properties);

  @Override
  public void onAfterTableRefreshed(AfterTableRefreshedEvent event) {
    HashMap<String, Object> properties = new HashMap<>();
    properties.put("event_type", event.getClass().getSimpleName());
    properties.put("table_identifier", event.tableIdentifier().toString());
    transformAndSendEvent(properties);
  }
}
