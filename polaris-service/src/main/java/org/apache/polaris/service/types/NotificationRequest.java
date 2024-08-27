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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.Objects;

@jakarta.annotation.Generated(
    value = "org.openapitools.codegen.languages.JavaResteasyServerCodegen",
    date = "2024-05-25T00:53:53.298853423Z[UTC]",
    comments = "Generator version: 7.5.0")
public class NotificationRequest {

  private NotificationType notificationType;
  private TableUpdateNotification payload;

  /** */
  @ApiModelProperty(required = true, value = "")
  @JsonProperty("notification-type")
  public NotificationType getNotificationType() {
    return notificationType;
  }

  public void setNotificationType(NotificationType notificationType) {
    this.notificationType = notificationType;
  }

  /** */
  @ApiModelProperty(value = "")
  @JsonProperty("payload")
  public TableUpdateNotification getPayload() {
    return payload;
  }

  public void setPayload(TableUpdateNotification payload) {
    this.payload = payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NotificationRequest notificationRequest = (NotificationRequest) o;
    return Objects.equals(this.notificationType, notificationRequest.notificationType)
        && Objects.equals(this.payload, notificationRequest.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(notificationType, payload);
  }

  @Override
  public String toString() {
    return """
        class NotificationRequest {
            notificationType: %s
            payload: %s
        }"""
        .formatted(toIndentedString(notificationType), toIndentedString(payload));
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces (except the first line).
   */
  private static String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
