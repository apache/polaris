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

package org.apache.polaris.service.events.listeners;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@ApplicationScoped
@Identifier("slack")
public class SlackPolarisEventListener implements PolarisEventListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlackPolarisEventListener.class);

    @Override
    public void onEvent(PolarisEvent event) {
        if (event.type() == PolarisEventType.AFTER_DELETE_CATALOG && event.attributes().getRequired(EventAttributes.CATALOG_NAME).startsWith("restricted")) {
            sendSlackEvent(event);
        }
    }


    /**
     * Slack UI URL: https://api.slack.com/apps/A0AA070U6PR/incoming-webhooks?success=1
     *
     * export CATALOG_NAME=catalog1
     * export RESTRICTED_CATALOG_NAME=restricted-catalog1
     * ./polaris --client-id root --client-secret s3cr3t catalogs create $CATALOG_NAME --storage-type FILE --default-base-location "/var/tmp/$CATALOG_NAME/"
     * ./polaris --client-id root --client-secret s3cr3t catalogs create $RESTRICTED_CATALOG_NAME --storage-type FILE --default-base-location "/var/tmp/$RESTRICTED_CATALOG_NAME/"
     *
     * ./polaris --client-id root --client-secret s3cr3t catalogs delete $CATALOG_NAME
     * ./polaris --client-id root --client-secret s3cr3t catalogs delete $RESTRICTED_CATALOG_NAME
     */
    private void sendSlackEvent(PolarisEvent event) {
        String payload = "";
        if (event.type() == PolarisEventType.AFTER_DELETE_CATALOG) {
            String catalogName = event.attributes().getRequired(EventAttributes.CATALOG_NAME);
            String principalName = event.metadata().user().map(PolarisPrincipal::getName).orElse(null);
            String requestId = event.metadata().requestId().orElse(null);

            payload = String.format("""
                {
                    "text": "Catalog *%s* deleted by *%s*, as part of request ID: *%s*"
                }
                """, catalogName, principalName, requestId);
        }

        if (payload.isEmpty()) {
            return;
        }

        LOGGER.debug("Sending Slack event: {}", payload);
        String webhookUrl = System.getenv("SLACK_WEBHOOK_URL");
        if (webhookUrl == null || webhookUrl.isBlank()) {
            throw new IllegalStateException("SLACK_WEBHOOK_URL environment variable is not set");
        }

        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(webhookUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());

            LOGGER.debug("Status: {}", response.statusCode());
            LOGGER.debug("Body: {}", response.body());
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
