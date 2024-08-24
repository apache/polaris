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
package io.polaris.core.persistence.cache;

/** Cache mode, the default is ENABLE. */
public enum EntityCacheMode {
  // bypass the cache, always load
  BYPASS,
  // enable the cache, this is the default
  ENABLE,
  // enable but verify that the cache content is consistent. Used in QA mode to detect when
  // versioning information is
  // not properly maintained
  ENABLE_BUT_VERIFY
}
