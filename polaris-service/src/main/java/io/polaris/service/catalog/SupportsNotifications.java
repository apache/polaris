package io.polaris.service.catalog;

import io.polaris.service.types.NotificationRequest;
import org.apache.iceberg.catalog.TableIdentifier;

public interface SupportsNotifications {

  public boolean sendNotification(TableIdentifier table, NotificationRequest notificationRequest);
}
