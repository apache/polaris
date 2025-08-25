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
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.service.events.jsonEventListener.JsonEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;

@ApplicationScoped
@Identifier("aws-cloudwatch")
public class AwsCloudWatchEventListener extends JsonEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(AwsCloudWatchEventListener.class);
  private final ObjectMapper objectMapper = new ObjectMapper();

  private CloudWatchLogsAsyncClient client;

  private final String logGroup;
  private final String logStream;
  private final Region region;
  private final boolean synchronousMode;
  private final Clock clock;

  @Inject CallContext callContext;

  @Context SecurityContext securityContext;

  @Inject
  public AwsCloudWatchEventListener(AwsCloudWatchConfiguration config, Clock clock) {
    this.logStream = config.awsCloudwatchlogStream();
    this.logGroup = config.awsCloudwatchlogGroup();
    this.region = Region.of(config.awsCloudwatchRegion());
    this.synchronousMode = config.synchronousMode();
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
    try {
      CompletableFuture<CreateLogGroupResponse> future =
          client.createLogGroup(CreateLogGroupRequest.builder().logGroupName(logGroup).build());
      future.join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof ResourceAlreadyExistsException) {
        LOGGER.debug("Log group {} already exists", logGroup);
      } else {
        throw e;
      }
    }

    try {
      CompletableFuture<CreateLogStreamResponse> future =
          client.createLogStream(
              CreateLogStreamRequest.builder()
                  .logGroupName(logGroup)
                  .logStreamName(logStream)
                  .build());
      future.join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof ResourceAlreadyExistsException) {
        LOGGER.debug("Log stream {} already exists", logStream);
      } else {
        throw e;
      }
    }
  }

  @PreDestroy
  void shutdown() {
    if (client != null) {
      client.close();
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
      LOGGER.error("Error processing event into JSON string: ", e);
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
