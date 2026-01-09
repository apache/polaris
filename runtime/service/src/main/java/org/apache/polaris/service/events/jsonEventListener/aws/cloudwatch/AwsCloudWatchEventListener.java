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

package org.apache.polaris.service.events.jsonEventListener.aws.cloudwatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

@ApplicationScoped
@Identifier("aws-cloudwatch")
public class AwsCloudWatchEventListener implements PolarisEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(AwsCloudWatchEventListener.class);

  private CloudWatchLogsAsyncClient client;

  private final String logGroup;
  private final String logStream;
  private final Region region;
  private final boolean synchronousMode;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  @Inject
  public AwsCloudWatchEventListener(
      AwsCloudWatchConfiguration config, ObjectMapper objectMapper, Clock clock) {
    this.logStream = config.awsCloudWatchLogStream();
    this.logGroup = config.awsCloudWatchLogGroup();
    this.region = Region.of(config.awsCloudWatchRegion());
    this.synchronousMode = config.synchronousMode();
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @PostConstruct
  void start() {
    this.client = createCloudWatchAsyncClient();
    ensureLogGroupAndStream();
  }

  protected CloudWatchLogsAsyncClient createCloudWatchAsyncClient() {
    return CloudWatchLogsAsyncClient.builder().region(region).build();
  }

  private void ensureLogGroupAndStream() {
    ensureResourceExists(
        () ->
            client
                .describeLogGroups(
                    DescribeLogGroupsRequest.builder().logGroupNamePrefix(logGroup).build())
                .join()
                .logGroups()
                .stream()
                .anyMatch(g -> g.logGroupName().equals(logGroup)),
        () ->
            client
                .createLogGroup(CreateLogGroupRequest.builder().logGroupName(logGroup).build())
                .join(),
        "group",
        logGroup);
    ensureResourceExists(
        () ->
            client
                .describeLogStreams(
                    DescribeLogStreamsRequest.builder()
                        .logGroupName(logGroup)
                        .logStreamNamePrefix(logStream)
                        .build())
                .join()
                .logStreams()
                .stream()
                .anyMatch(s -> s.logStreamName().equals(logStream)),
        () ->
            client
                .createLogStream(
                    CreateLogStreamRequest.builder()
                        .logGroupName(logGroup)
                        .logStreamName(logStream)
                        .build())
                .join(),
        "stream",
        logStream);
  }

  private static void ensureResourceExists(
      Supplier<Boolean> existsCheck,
      Runnable createAction,
      String resourceType,
      String resourceName) {
    if (existsCheck.get()) {
      LOGGER.debug("Log {} [{}] already exists", resourceType, resourceName);
    } else {
      LOGGER.debug("Attempting to create log {}: {}", resourceType, resourceName);
      createAction.run();
    }
  }

  @PreDestroy
  void shutdown() {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  @Override
  public void onEvent(PolarisEvent event) {
    if (event.type() != PolarisEventType.AFTER_REFRESH_TABLE) {
      return;
    }
    HashMap<String, Object> properties = new HashMap<>();
    properties.put("event_type", event.type().name());
    event
        .attributes()
        .get(EventAttributes.TABLE_IDENTIFIER)
        .map(TableIdentifier::toString)
        .ifPresent(id -> properties.put("table_identifier", id));

    properties.put("realm_id", event.metadata().realmId());
    event
        .metadata()
        .user()
        .ifPresent(
            p -> {
              properties.put("principal", p.getName());
              properties.put("activated_roles", p.getRoles());
            });
    event.metadata().requestId().ifPresent(id -> properties.put("request_id", id));

    String eventAsJson;
    try {
      eventAsJson = objectMapper.writeValueAsString(properties);
    } catch (JsonProcessingException e) {
      LOGGER.error("Error processing event into JSON string: ", e);
      LOGGER.debug("Failed to convert the following object into JSON string: {}", properties);
      return;
    }
    InputLogEvent inputLogEvent =
        InputLogEvent.builder().message(eventAsJson).timestamp(clock.millis()).build();
    PutLogEventsRequest.Builder requestBuilder =
        PutLogEventsRequest.builder()
            .logGroupName(logGroup)
            .logStreamName(logStream)
            .logEvents(List.of(inputLogEvent));
    CompletableFuture<PutLogEventsResponse> future =
        client
            .putLogEvents(requestBuilder.build())
            .whenComplete(
                (resp, err) -> {
                  if (err != null) {
                    LOGGER.error(
                        "Error writing log to CloudWatch. Event: {}, Error: ", inputLogEvent, err);
                  }
                });
    if (synchronousMode) {
      future.join();
    }
  }
}
