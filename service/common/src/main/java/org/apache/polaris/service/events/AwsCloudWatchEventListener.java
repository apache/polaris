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
import com.fasterxml.jackson.databind.ObjectMapper;
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

import org.apache.polaris.core.context.CallContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidSequenceTokenException;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;

@ApplicationScoped
@Identifier("aws-cloudwatch")
public class AwsCloudWatchEventListener extends PolarisEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(AwsCloudWatchEventListener.class);
  private final ObjectMapper objectMapper = new ObjectMapper();
  private static final int MAX_BATCH_SIZE = 10_000;
  private static final int MAX_WAIT_MS = 5000;

  private final BlockingQueue<EventAndTimestamp> queue = new LinkedBlockingQueue<>();
  private CloudWatchLogsClient client;
  private volatile String sequenceToken;

  private volatile boolean running = true;

  ExecutorService executorService;

  private Future<?> backgroundTask;

  private final String logGroup;
  private final String logStream;
  private final Region region;

  @Inject
  public AwsCloudWatchEventListener(EventListenerConfiguration config, ExecutorService executorService) {
    this.executorService = executorService;

    this.logStream = config.awsCloudwatchlogStream().orElse("polaris-cloudwatch-default-stream");
    this.logGroup = config.awsCloudwatchlogGroup().orElse("polaris-cloudwatch-default-group");
    this.region = Region.of(config.awsCloudwatchRegion().orElse("us-east-1"));
  }

  @PostConstruct
  void start() {
    client = CloudWatchLogsClient.builder().region(region).build();
    ensureLogGroupAndStream();
    backgroundTask = executorService.submit(this::processQueue);
  }

  private void processQueue() {
    while (running || !queue.isEmpty()) {
      List<InputLogEvent> events = new ArrayList<>();
      try {
        EventAndTimestamp first = queue.poll(MAX_WAIT_MS, TimeUnit.MILLISECONDS);

        if (first != null) {
          events.add(createLogEvent(first));
          List<EventAndTimestamp> drainedEvents = new ArrayList<>();
          queue.drainTo(drainedEvents, MAX_BATCH_SIZE - 1);
          drainedEvents.forEach(event -> events.add(createLogEvent(event)));
        }

        if (!events.isEmpty()) {
          sendToCloudWatch(events);
        }
      } catch (Exception e) {
        LOGGER.error("Error writing logs to CloudWatch: {}", e.getMessage());
        LOGGER.error("Events not logged: {}", events);
      }
    }
  }

  private InputLogEvent createLogEvent(EventAndTimestamp eventAndTimestamp) {
    return InputLogEvent.builder()
        .message(eventAndTimestamp.event)
        .timestamp(eventAndTimestamp.timestamp)
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
        PutLogEventsResponse response = client.putLogEvents(requestBuilder.build());
        sequenceToken = response.nextSequenceToken();
      } catch (InvalidSequenceTokenException e) {
        sequenceToken = getSequenceToken();
        requestBuilder.sequenceToken(sequenceToken);
        PutLogEventsResponse retryResponse = client.putLogEvents(requestBuilder.build());
        sequenceToken = retryResponse.nextSequenceToken();
      }
    }
  }

  private void ensureLogGroupAndStream() {
    try {
      client.createLogGroup(CreateLogGroupRequest.builder().logGroupName(logGroup).build());
    } catch (ResourceAlreadyExistsException ignored) {
    }

    try {
      client.createLogStream(
          CreateLogStreamRequest.builder().logGroupName(logGroup).logStreamName(logStream).build());
    } catch (ResourceAlreadyExistsException ignored) {
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

  private record EventAndTimestamp(String event, long timestamp) {}

  // Event overrides below
  @Override
  public void onAfterCatalogCreated(AfterCatalogCreatedEvent event, CallContext callContext) {
    try {
      Map<String, Object> json = objectMapper.convertValue(event.catalog(), Map.class);
      json.put("realm", callContext.getRealmContext().getRealmIdentifier());
      json.put("event_type", event.getClass().getSimpleName());
      queue.add(
          new EventAndTimestamp(
              objectMapper.writeValueAsString(json), System.currentTimeMillis()));
    } catch (JsonProcessingException e) {
      LOGGER.error("Error processing event into JSON string: {}", e.getMessage());
    }
  }
}
