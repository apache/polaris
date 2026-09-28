---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: smallrye-polaris_event_listener_kafka
build:
  list: never
  render: never
---

Configuration for the Kafka Event Listener. 

Publishes Polaris events to a Kafka topic. The key for the Kafka record is the UUID of the  Polaris event. To preserve FIFO behavior in the Kafka stream of the Polaris events a single  partition should be used.   

The value of Kafka record is JSON encoded as a String. It consists of a set of key values. The  set of keys included depends on the Polaris event type and are listed below. The type of value is  String except for keys source, destination, table_identifier, view_identifier, namespace and  activated_roles which are all encoded as an array of Strings.   

 * event_type    
 * timestamp (ISO-8601 representation)    
 * source (source of table rename if the event is a table rename)    
 * destination (destination of table rename if the event is a table rename)    
 * table_name    
 * namespace    
 * table_identifier    
 * view_identifier    
 * view_name    
 * namespace_name    
 * catalog_name    
 * realm_id    
 * principal    
 * activated_roles    
 * request_id

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.event-listener.kafka.topic` | `polaris-catalog-events` | `string` | The Kafka topic to send Polaris events to. <br><br>Configuration property: `polaris.event-listener.kafka.topic` |
| `polaris.event-listener.kafka.synchronous-mode` | `false` | `boolean` | The synchronous mode setting for sending events to Kafka. <br><br>When set to "true", when events are sent to Kafka the event handling thread will wait for  the events to be delivered to Kafka, which may impact performance. When set to "false"  (default), events are sent asynchronously.   <br><br>Configuration property: `polaris.event-listener.kafka.synchronous-mode` |
| `polaris.event-listener.kafka.properties.`_`<name>`_ |  | `string` | Kafka properties to pass to the Kafka client producer, for example bootstrap.servers is  required.  This can be used to configure authentication (e.g., SASL, SSL) or other Kafka  producer properties.   <br><br>Configuration property: `polaris.event-listener.kafka.properties` |
