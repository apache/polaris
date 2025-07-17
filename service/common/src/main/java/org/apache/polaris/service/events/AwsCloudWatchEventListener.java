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

package org.apache.polaris.service.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.polaris.core.context.CallContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DataAlreadyAcceptedException;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidParameterException;
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidSequenceTokenException;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.UnrecognizedClientException;

@ApplicationScoped
@Identifier("aws-cloudwatch")
public class AwsCloudWatchEventListener extends PolarisEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(AwsCloudWatchEventListener.class);
  private final ObjectMapper objectMapper = new ObjectMapper();
  private static final int MAX_BATCH_SIZE = 10_000;
  private static final int MAX_WAIT_MS = 5000;
  private static final String DEFAULT_LOG_STREAM_NAME = "polaris-cloudwatch-default-stream";
  private static final String DEFAULT_LOG_GROUP_NAME = "polaris-cloudwatch-default-group";
  private static final String DEFAULT_REGION = "us-east-1";

  private final BlockingQueue<EventWithTimestamp> queue = new LinkedBlockingQueue<>();
  private CloudWatchLogsClient client;
  private volatile String sequenceToken;

  private volatile boolean running = true;

  ExecutorService executorService;

  private Future<?> backgroundTask;

  private final String logGroup;
  private final String logStream;
  private final Region region;

  @Inject
  CallContext callContext;

  @Context
  SecurityContext securityContext;

  @Inject
  public AwsCloudWatchEventListener(
      EventListenerConfiguration config, ExecutorService executorService) {
    this.executorService = executorService;

    this.logStream = config.awsCloudwatchlogStream().orElse(DEFAULT_LOG_STREAM_NAME);
    this.logGroup = config.awsCloudwatchlogGroup().orElse(DEFAULT_LOG_GROUP_NAME);
    this.region = Region.of(config.awsCloudwatchRegion().orElse(DEFAULT_REGION));
  }

  @PostConstruct
  void start() {
    this.client = createCloudWatchClient();
    ensureLogGroupAndStream();
    backgroundTask = executorService.submit(this::processQueue);
  }

  protected CloudWatchLogsClient createCloudWatchClient() {
    return CloudWatchLogsClient.builder().region(region).build();
  }

  private void processQueue() {
    List<EventWithTimestamp> drainedEvents = new ArrayList<>();
    List<InputLogEvent> transformedEvents = new ArrayList<>();
    while (running || !queue.isEmpty()) {
      drainQueue(drainedEvents, transformedEvents);
    }
  }

  @VisibleForTesting
  public void drainQueue(
      List<EventWithTimestamp> drainedEvents, List<InputLogEvent> transformedEvents) {
    try {
      EventWithTimestamp first = queue.poll(MAX_WAIT_MS, TimeUnit.MILLISECONDS);

      if (first != null) {
        drainedEvents.add(first);
        queue.drainTo(drainedEvents, MAX_BATCH_SIZE - 1);
      } else {
        return;
      }

      drainedEvents.forEach(event -> transformedEvents.add(createLogEvent(event)));

      sendToCloudWatch(transformedEvents);
    } catch (InterruptedException e) {
      if (!queue.isEmpty()) {
        LOGGER.debug("Interrupted while waiting for queue to drain", e);
        queue.addAll(drainedEvents);
      }
    } catch (DataAlreadyAcceptedException e) {
      LOGGER.debug("Data already accepted: {}", e.getMessage());
    } catch (RuntimeException e) {
      if (e instanceof SdkClientException
          || e instanceof InvalidParameterException
          || e instanceof UnrecognizedClientException) {
        LOGGER.error(
            "Error writing logs to CloudWatch - client-side error. Please adjust Polaris configurations: {}",
            e.getMessage());
      } else {
        LOGGER.error("Error writing logs to CloudWatch - server-side error: {}", e.getMessage());
      }
      LOGGER.error("Number of dropped events: {}", transformedEvents.size());
      LOGGER.debug("Events not logged: {}", transformedEvents);
      queue.addAll(drainedEvents);
    }
    drainedEvents.clear();
    transformedEvents.clear();
  }

  private InputLogEvent createLogEvent(EventWithTimestamp eventWithTimestamp) {
    return InputLogEvent.builder()
        .message(eventWithTimestamp.event)
        .timestamp(eventWithTimestamp.timestamp)
        .build();
  }

  private void sendToCloudWatch(List<InputLogEvent> events) {
    events.sort(Comparator.comparingLong(InputLogEvent::timestamp));

    PutLogEventsRequest.Builder requestBuilder =
        PutLogEventsRequest.builder()
            .logGroupName(logGroup)
            .logStreamName(logStream)
            .logEvents(events);

    synchronized (this) {
      if (sequenceToken != null) {
        requestBuilder.sequenceToken(sequenceToken);
      }

      try {
        executePutLogEvents(requestBuilder);
      } catch (InvalidSequenceTokenException e) {
        sequenceToken = getSequenceToken();
        requestBuilder.sequenceToken(sequenceToken);
        executePutLogEvents(requestBuilder);
      }
    }
  }

  private void executePutLogEvents(PutLogEventsRequest.Builder requestBuilder) {
    PutLogEventsResponse response = client.putLogEvents(requestBuilder.build());
    sequenceToken = response.nextSequenceToken();
  }

  private void ensureLogGroupAndStream() {
    try {
      client.createLogGroup(CreateLogGroupRequest.builder().logGroupName(logGroup).build());
    } catch (ResourceAlreadyExistsException ignored) {
      LOGGER.debug("Log group {} already exists", logGroup);
    }

    try {
      client.createLogStream(
          CreateLogStreamRequest.builder().logGroupName(logGroup).logStreamName(logStream).build());
    } catch (ResourceAlreadyExistsException ignored) {
      LOGGER.debug("Log stream {} already exists", logStream);
    }

    sequenceToken = getSequenceToken();
  }

  private String getSequenceToken() {
    DescribeLogStreamsResponse response =
        client.describeLogStreams(
            DescribeLogStreamsRequest.builder()
                .logGroupName(logGroup)
                .logStreamNamePrefix(logStream)
                .build());

    return response.logStreams().stream()
        .filter(s -> logStream.equals(s.logStreamName()))
        .map(LogStream::uploadSequenceToken)
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  @PreDestroy
  void shutdown() {
    running = false;
    if (backgroundTask != null) {
      try {
        backgroundTask.get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOGGER.error("Error waiting for background logging task to finish: {}", e.getMessage());
      }
    }
    if (client != null) {
      client.close();
    }
  }

  record EventWithTimestamp(String event, long timestamp) {}

  private long getCurrentTimestamp(CallContext callContext) {
    return callContext.getPolarisCallContext().getClock().millis();
  }

  // Event overrides below
  private void queueEvent(PolarisEvent event) {
    try {
      Map<String, Object> json = objectMapper.convertValue(event, new TypeReference<>() {});
      json.put("realm", callContext.getRealmContext().getRealmIdentifier());
      json.put("event_type", event.getClass().getSimpleName());
      json.put("principal", securityContext.getUserPrincipal().getName());
      queue.add(
          new EventWithTimestamp(
              objectMapper.writeValueAsString(json), getCurrentTimestamp(callContext)));
    } catch (JsonProcessingException e) {
      LOGGER.error("Error processing event into JSON string: {}", e.getMessage());
    }
  }

  @Override
  public void onAfterCatalogCreated(AfterCatalogCreatedEvent event) {
    queueEvent(event);
  }
}
