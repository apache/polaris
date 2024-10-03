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
package org.apache.polaris.service.types;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public enum NotificationType {

  /** Supported notification types for the update table notification. */
  UNKNOWN(0, "UNKNOWN"),
  CREATE(1, "CREATE"),
  UPDATE(2, "UPDATE"),
  DROP(3, "DROP"),
  VALIDATE(4, "VALIDATE");

  NotificationType(int id, String displayName) {
    this.id = id;
    this.displayName = displayName;
  }

  /** Internal id of the notification type. */
  private final int id;

  /** Display name of the notification type */
  private final String displayName;

  /** Internal ids and their corresponding sources of notification types. */
  private static final Map<Integer, NotificationType> idToNotificationTypeMap =
      Arrays.stream(NotificationType.values())
          .collect(Collectors.toMap(NotificationType::getId, tf -> tf));

  /**
   * Lookup a notification type using its internal id representation
   *
   * @param id internal id of the notification type
   * @return The notification type, if it exists, or empty
   */
  public static Optional<NotificationType> lookupById(int id) {
    return Optional.ofNullable(idToNotificationTypeMap.get(id));
  }

  /**
   * Return the internal id of the notification type
   *
   * @return id
   */
  public int getId() {
    return id;
  }

  /** Return the display name of the notification type */
  public String getDisplayName() {
    return displayName;
  }

  /**
   * Find the notification type by name, or return an empty optional
   *
   * @param name name of the notification type
   * @return The notification type, if it exists, or empty
   */
  public static Optional<NotificationType> lookupByName(String name) {
    if (name == null) {
      return Optional.empty();
    }

    for (NotificationType NotificationType : NotificationType.values()) {
      if (name.toUpperCase(Locale.ROOT).equals(NotificationType.name())) {
        return Optional.of(NotificationType);
      }
    }
    return Optional.empty();
  }
}
