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
package org.apache.polaris.core.storage;

/** Constants for internal properties used in storage integrations */
public class StorageInternalProperties {
  /** Property key for identifying the storage type in internal properties */
  public static final String STORAGE_TYPE_KEY = "storageType";

  /** Property key for HDFS configuration resources in internal properties */
  public static final String HDFS_CONFIG_RESOURCES_KEY = "hdfs.config-resources";

  /** Property key for HDFS username in internal properties */
  public static final String HDFS_USERNAME_KEY = "hdfs.username";
}
