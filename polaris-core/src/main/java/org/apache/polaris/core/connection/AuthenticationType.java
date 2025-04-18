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
package org.apache.polaris.core.connection;

/**
 * The internal persistence-object counterpart to AuthenticationParameters.AuthenticationTypeEnum
 * defined in the API model. We define integer type codes in this enum for better compatibility
 * within persisted data in case the names of enum types are ever changed in place.
 *
 * <p>Important: Codes must be kept in-sync with JsonSubTypes annotated within {@link
 * AuthenticationParametersDpo}.
 */
public enum AuthenticationType {
  OAUTH(1),
  BEARER(2);

  private final int code;

  AuthenticationType(int code) {
    this.code = code;
  }

  public int getCode() {
    return this.code;
  }
}
