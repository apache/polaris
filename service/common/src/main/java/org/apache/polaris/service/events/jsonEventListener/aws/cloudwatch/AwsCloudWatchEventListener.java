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
import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.service.events.AfterTableRefreshedEvent;
import org.apache.polaris.service.events.PolarisEventListener;
import org.apache.polaris.service.events.jsonEventListener.JsonEventListener;
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
public class AwsCloudWatchEventListener extends JsonEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(AwsCloudWatchEventListener.class);
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final ConcurrentHashMap<Future<?>, EventWithRetry> futures = new ConcurrentHashMap<>();

  record EventWithRetry(InputLogEvent inputLogEvent, int retryCount) {}

  private CloudWatchLogsClient client;
  private volatile String sequenceToken;

  ExecutorService executorService;

  private final String logGroup;
  private final String logStream;
  private final Region region;
  private final boolean synchronousMode;

  @Inject CallContext callContext;

  @Context SecurityContext securityContext;

  @Inject
  public AwsCloudWatchEventListener(
      AwsCloudWatchConfiguration config, ExecutorService executorService) {
    this.executorService = executorService;

    this.logStream =
        config
            .awsCloudwatchlogStream()
            .orElseThrow(
                () -> new IllegalArgumentException("AWS CloudWatch log stream must be configured"));
    this.logGroup =
        config
            .awsCloudwatchlogGroup()
            .orElseThrow(
                () -> new IllegalArgumentException("AWS CloudWatch log group must be configured"));
    this.region =
        Region.of(
            config
                .awsCloudwatchRegion()
                .orElseThrow(
                    () ->
                        new IllegalArgumentException("AWS CloudWatch region must be configured")));
    this.synchronousMode = Boolean.parseBoolean(config.synchronousMode());
  }

  @PostConstruct
  void start() {
    this.client = createCloudWatchClient();
    ensureLogGroupAndStream();
  }

  protected CloudWatchLogsClient createCloudWatchClient() {
    return CloudWatchLogsClient.builder().region(region).build();
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
    for (Future<?> future : futures.keySet()) {
      if (!future.state().equals(Future.State.SUCCESS)) {
        LOGGER.debug(
            "Event not emitted to AWS CloudWatch due to being in state {}: {}",
            future.state(),
            futures.get(future).inputLogEvent);
        future.cancel(true);
      }
    }
    if (client != null) {
      client.close();
    }
  }

  private void sendAndHandleCloudWatchCall(InputLogEvent event) {
    try {
      sendToCloudWatch(event);
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
      throw e;
    } finally {
      try {
        reapFuturesMap();
      } catch (Exception e) {
        LOGGER.debug("Futures map could not be reaped: {}", e.getMessage());
      }
    }
  }

  private void sendToCloudWatch(InputLogEvent event) {
    PutLogEventsRequest.Builder requestBuilder =
        PutLogEventsRequest.builder()
            .logGroupName(logGroup)
            .logStreamName(logStream)
            .logEvents(List.of(event));

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

  private void reapFuturesMap() {
    for (Future<?> future : futures.keySet()) {
      if (future.isDone()) {
        Future.State currFutureState = future.state();
        EventWithRetry currValue = futures.remove(future);
        if (currFutureState.equals(Future.State.FAILED)
            || currFutureState.equals(Future.State.CANCELLED)) {
          if (currValue.retryCount >= 3) {
            LOGGER.error("Event retries failed. Event dropped: {}", currValue.inputLogEvent);
          } else {
            EventWithRetry newValue =
                new EventWithRetry(currValue.inputLogEvent, currValue.retryCount + 1);
            future =
                executorService.submit(() -> sendAndHandleCloudWatchCall(newValue.inputLogEvent));
            futures.put(future, newValue);
          }
        }
      }
    }
  }

  @Override
  protected void transformAndSendEvent(HashMap<String, Object> properties) {
    properties.put("realm", callContext.getRealmContext().getRealmIdentifier());
    properties.put("principal", securityContext.getUserPrincipal().getName());
    // TODO: Add request ID when it is available
    String eventAsJson;
    try {
      eventAsJson = objectMapper.writeValueAsString(properties);
    } catch (JsonProcessingException e) {
      LOGGER.error("Error processing event into JSON string: {}", e.getMessage());
      return;
    }
    InputLogEvent inputLogEvent = createLogEvent(eventAsJson, getCurrentTimestamp(callContext));
    if (!synchronousMode) {
      Future<?> future = executorService.submit(() -> sendAndHandleCloudWatchCall(inputLogEvent));
      futures.put(future, new EventWithRetry(inputLogEvent, 0));
    } else {
      sendAndHandleCloudWatchCall(inputLogEvent);
    }
  }

  private long getCurrentTimestamp(CallContext callContext) {
    return callContext.getPolarisCallContext().getClock().millis();
  }

  private InputLogEvent createLogEvent(String eventAsJson, long timestamp) {
    return InputLogEvent.builder().message(eventAsJson).timestamp(timestamp).build();
  }

  @VisibleForTesting
  ConcurrentHashMap<Future<?>, EventWithRetry> getFutures() {
    return futures;
  }
}
