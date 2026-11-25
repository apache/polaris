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
package org.apache.polaris.extension.auth.ranger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.config.ProductionReadinessCheck.Error;

@ApplicationScoped
public class RangerProductionReadinessChecks {

  @Produces
  public ProductionReadinessCheck checkRangerLegacyProperties(
      RangerPolarisAuthorizerConfig rangerConfig) {
    List<Error> errors = new ArrayList<>();
    for (String key : new TreeSet<>(rangerConfig.legacyProperties().keySet())) {
      errors.add(
          Error.of(
              "Ranger properties set directly under polaris.authorization.ranger.* are deprecated. "
                  + "Use polaris.authorization.ranger.properties."
                  + key
                  + " instead.",
              "polaris.authorization.ranger." + key));
    }
    return ProductionReadinessCheck.of(errors);
  }
}
