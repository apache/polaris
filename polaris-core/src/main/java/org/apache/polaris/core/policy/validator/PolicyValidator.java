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
package org.apache.polaris.core.policy.validator;

import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;

/**
 * Validates and parses a given policy content string against its defined schema.
 *
 * @param <T> the type of policy object to be returned after validation
 */
public interface PolicyValidator<T> {

  /**
   * Parses and validates the provided policy content.
   *
   * @param content the policy content to parse and validate
   * @return a policy object of type T if the content is valid
   * @throws InvalidPolicyException if the content does not meet the required policy rules
   */
  T parse(String content) throws InvalidPolicyException;

  boolean canAttach(PolarisEntityType entityType, PolarisEntitySubType entitySubType);
}
