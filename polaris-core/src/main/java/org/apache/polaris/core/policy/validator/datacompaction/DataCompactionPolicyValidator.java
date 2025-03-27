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
package org.apache.polaris.core.policy.validator.datacompaction;

import static org.apache.polaris.core.entity.PolarisEntityType.CATALOG;
import static org.apache.polaris.core.entity.PolarisEntityType.NAMESPACE;
import static org.apache.polaris.core.entity.PolarisEntityType.TABLE_LIKE;

import java.util.Set;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.policy.validator.InvalidPolicyException;
import org.apache.polaris.core.policy.validator.PolicyValidator;

public class DataCompactionPolicyValidator implements PolicyValidator {
  public static final DataCompactionPolicyValidator INSTANCE = new DataCompactionPolicyValidator();

  private static final Set<PolarisEntityType> ATTACHABLE_ENTITY_TYPES =
      Set.of(CATALOG, NAMESPACE, TABLE_LIKE);

  @Override
  public void validate(String content) throws InvalidPolicyException {
    DataCompactionPolicyContent.fromString(content);
  }

  @Override
  public boolean canAttach(PolarisEntityType entityType, PolarisEntitySubType entitySubType) {
    if (entityType == null) {
      return false;
    }

    if (!ATTACHABLE_ENTITY_TYPES.contains(entityType)) {
      return false;
    }

    if (entityType == TABLE_LIKE && entitySubType != PolarisEntitySubType.ICEBERG_TABLE) {
      return false;
    }

    return true;
  }
}
