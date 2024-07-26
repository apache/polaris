package io.polaris.service.types;

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
    StringBuilder sb = new StringBuilder();
    sb.append("class NotificationRequest {\n");

    sb.append("    notificationType: ").append(toIndentedString(notificationType)).append("\n");
    sb.append("    payload: ").append(toIndentedString(payload)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
