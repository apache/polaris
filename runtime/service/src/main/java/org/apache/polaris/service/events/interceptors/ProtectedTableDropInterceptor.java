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

package org.apache.polaris.service.events.interceptors;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Locale;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventInterceptor;
import org.apache.polaris.service.events.PolarisEventType;

/** Denies dropping tables with a configured protection tag encoded in table names. */
@ApplicationScoped
@Identifier("protected-table-drop")
public class ProtectedTableDropInterceptor implements PolarisEventInterceptor {
  private final ProtectedTableDropInterceptorConfiguration configuration;

  @Inject
  public ProtectedTableDropInterceptor(ProtectedTableDropInterceptorConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Result intercept(PolarisEvent event) {
    if (!isDropTableEvent(event.type())) {
      return Result.allow();
    }

    String tableName =
        event
            .attributes()
            .get(EventAttributes.GENERIC_TABLE_NAME)
            .or(() -> event.attributes().get(EventAttributes.TABLE_NAME))
            .orElse(null);

    if (tableName == null) {
      return Result.allow();
    }

    if (containsProtectedTag(tableName, configuration.tagToken(), configuration.separator())) {
      return Result.deny("Drop denied for protected table '" + tableName + "'");
    }

    return Result.allow();
  }

  static boolean isDropTableEvent(PolarisEventType eventType) {
    return eventType == PolarisEventType.BEFORE_DROP_GENERIC_TABLE
        || eventType == PolarisEventType.BEFORE_DROP_TABLE;
  }

  static boolean containsProtectedTag(String tableName, String tagToken, String separator) {
    if (tableName == null || tableName.isBlank() || tagToken == null || tagToken.isBlank()) {
      return false;
    }

    String normalizedName = tableName.toLowerCase(Locale.ROOT);
    String normalizedToken = tagToken.toLowerCase(Locale.ROOT);

    if (separator == null || separator.isBlank()) {
      return normalizedName.contains(normalizedToken);
    }

    return normalizedName.equals(normalizedToken)
        || normalizedName.startsWith(normalizedToken + separator)
        || normalizedName.endsWith(separator + normalizedToken)
        || normalizedName.contains(separator + normalizedToken + separator);
  }
}
