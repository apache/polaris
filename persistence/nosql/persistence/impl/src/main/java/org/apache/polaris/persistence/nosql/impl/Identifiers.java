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
package org.apache.polaris.persistence.nosql.impl;

/** Common identifiers used in the various database-specific implementations. */
public final class Identifiers {
  private Identifiers() {}

  // Use short column/field names that are as short as possible.

  public static final String COL_REALM = "r";

  public static final String TABLE_REFS = "refs";

  public static final String COL_REF_NAME = "n";
  public static final String COL_REF_POINTER = "p";
  public static final String COL_REF_CREATED_AT = "c";
  public static final String COL_REF_PREVIOUS = "t";

  public static final String TABLE_OBJS = "objs";

  public static final String COL_OBJ_TYPE = "t";
  public static final String COL_OBJ_ID = "i";
  public static final String COL_OBJ_PART = "p";
  public static final String COL_OBJ_REAL_PART_NUM = "q";
  public static final String COL_OBJ_VALUE = "d";
  public static final String COL_OBJ_VERSION = "v";
  public static final String COL_OBJ_CREATED_AT = "c";
}
