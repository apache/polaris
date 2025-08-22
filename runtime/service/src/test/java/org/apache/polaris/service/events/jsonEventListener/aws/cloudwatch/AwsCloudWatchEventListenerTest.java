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

import static org.apache.polaris.containerspec.ContainerSpecHelper.containerSpecHelper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.events.AfterTableRefreshedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsAsyncClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;

class AwsCloudWatchEventListenerTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AwsCloudWatchEventListenerTest.class);

  private static final LocalStackContainer localStack =
      new LocalStackContainer(
              containerSpecHelper("localstack", AwsCloudWatchEventListenerTest.class)
                  .dockerImageName(null))
          .withServices(LocalStackContainer.Service.CLOUDWATCHLOGS);

  private static final String LOG_GROUP = "test-log-group";
  private static final String LOG_STREAM = "test-log-stream";
  private static final String REALM = "test-realm";
  private static final String TEST_USER = "test-user";
  private static final Clock clock = Clock.systemUTC();

  @Mock private AwsCloudWatchConfiguration config;

  private ExecutorService executorService;
  private AutoCloseable mockitoContext;

  @BeforeEach
  void setUp() {
    mockitoContext = MockitoAnnotations.openMocks(this);
    executorService = Executors.newSingleThreadExecutor();

    // Configure the mocks
    when(config.awsCloudwatchlogGroup()).thenReturn(LOG_GROUP);
    when(config.awsCloudwatchlogStream()).thenReturn(LOG_STREAM);
    when(config.awsCloudwatchRegion()).thenReturn("us-east-1");
    when(config.synchronousMode()).thenReturn(false); // Default to async mode
  }

  @AfterEach
  void tearDown() throws Exception {
    if (mockitoContext != null) {
      mockitoContext.close();
    }
    if (executorService != null) {
      executorService.shutdownNow();
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        LOGGER.warn("ExecutorService did not terminate in time");
      }
    }
    if (localStack.isRunning()) {
      localStack.stop();
    }
  }

  private CloudWatchLogsAsyncClient createCloudWatchAsyncClient() {
    if (!localStack.isRunning()) {
      localStack.start();
    }
    return CloudWatchLogsAsyncClient.builder()
        .endpointOverride(localStack.getEndpoint())
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
        .region(Region.of(localStack.getRegion()))
        .build();
  }

  private AwsCloudWatchEventListener createListener(CloudWatchLogsAsyncClient client) {
    AwsCloudWatchEventListener listener =
        new AwsCloudWatchEventListener(config, clock) {
          @Override
          protected CloudWatchLogsAsyncClient createCloudWatchAsyncClient() {
            return client;
          }
        };

    // Set up call context and security context
    CallContext callContext = Mockito.mock(CallContext.class);
    PolarisCallContext polarisCallContext = Mockito.mock(PolarisCallContext.class);
    RealmContext realmContext = Mockito.mock(RealmContext.class);
    SecurityContext securityContext = Mockito.mock(SecurityContext.class);
    Principal principal = Mockito.mock(Principal.class);
    when(callContext.getRealmContext()).thenReturn(realmContext);
    when(callContext.getPolarisCallContext()).thenReturn(polarisCallContext);
    when(realmContext.getRealmIdentifier()).thenReturn(REALM);
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(principal.getName()).thenReturn(TEST_USER);
    listener.callContext = callContext;
    listener.securityContext = securityContext;

    return listener;
  }

  @Test
  void shouldCreateLogGroupAndStream() {
    CloudWatchLogsAsyncClient client = createCloudWatchAsyncClient();
    AwsCloudWatchEventListener listener = createListener(client);

    // Start the listener which should create the log group and stream
    listener.start();

    try {
      // Verify log group exists
      DescribeLogGroupsResponse groups =
              client
                      .describeLogGroups(
                              DescribeLogGroupsRequest.builder().logGroupNamePrefix(LOG_GROUP).build())
                      .join();
      assertThat(groups.logGroups())
              .hasSize(1)
              .first()
              .satisfies(group -> assertThat(group.logGroupName()).isEqualTo(LOG_GROUP));

      // Verify log stream exists
      DescribeLogStreamsResponse streams =
              client
                      .describeLogStreams(
                              DescribeLogStreamsRequest.builder()
                                      .logGroupName(LOG_GROUP)
                                      .logStreamNamePrefix(LOG_STREAM)
                                      .build())
                      .join();
      assertThat(streams.logStreams())
              .hasSize(1)
              .first()
              .satisfies(stream -> assertThat(stream.logStreamName()).isEqualTo(LOG_STREAM));
    } finally {
      // Clean up
      listener.shutdown();
      client.close();
    }
  }

  @Test
  void shouldSendEventToCloudWatch() {
    CloudWatchLogsAsyncClient client = createCloudWatchAsyncClient();
    AwsCloudWatchEventListener listener = createListener(client);
    listener.start();
    try {
      // Create and send a test event
      TableIdentifier testTable = TableIdentifier.of("test_namespace", "test_table");
      AfterTableRefreshedEvent event = new AfterTableRefreshedEvent(testTable);
      listener.onAfterTableRefreshed(event);

      // Verify the event was sent to CloudWatch by reading the actual logs
      GetLogEventsResponse logEvents;
      int eventCount = 0;
      long startTime = System.currentTimeMillis();
      while (eventCount == 0) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          fail("Thread interrupted waiting for event");
        }
        if (System.currentTimeMillis() - startTime > 30_000) {
          fail("Timeout exceeded while waiting for event to arrive");
        }
        logEvents =
                client.getLogEvents(
                                GetLogEventsRequest.builder()
                                        .logGroupName(LOG_GROUP)
                                        .logStreamName(LOG_STREAM)
                                        .build())
                        .join();
        eventCount = logEvents.events().size();
      }

      logEvents =
              client
                      .getLogEvents(
                              GetLogEventsRequest.builder()
                                      .logGroupName(LOG_GROUP)
                                      .logStreamName(LOG_STREAM)
                                      .build())
                      .join();

      assertThat(logEvents.events())
              .hasSize(1)
              .first()
              .satisfies(
                      logEvent -> {
                        String message = logEvent.message();
                        assertThat(message).contains(REALM);
                        assertThat(message).contains(AfterTableRefreshedEvent.class.getSimpleName());
                        assertThat(message).contains(TEST_USER);
                        assertThat(message).contains(testTable.toString());
                      });
    } finally {
      // Clean up
      listener.shutdown();
      client.close();
    }
  }

  @Test
  void shouldSendEventInSynchronousMode() {
    CloudWatchLogsAsyncClient client = createCloudWatchAsyncClient();

    // Test synchronous mode
    when(config.synchronousMode()).thenReturn(true);
    AwsCloudWatchEventListener syncListener = createListener(client);
    syncListener.start();
    try {
      // Create and send a test event synchronously
      TableIdentifier syncTestTable = TableIdentifier.of("test_namespace", "test_table_sync");
      AfterTableRefreshedEvent syncEvent = new AfterTableRefreshedEvent(syncTestTable);
      syncListener.onAfterTableRefreshed(syncEvent);

      // Verify both events were sent to CloudWatch
      GetLogEventsResponse logEvents;
      int eventCount = 0;
      long startTime = System.currentTimeMillis();
      while (eventCount == 0) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          fail("Thread interrupted waiting for event");
        }
        if (System.currentTimeMillis() - startTime > 30_000) {
          fail("Timeout exceeded while waiting for event to arrive");
        }
        logEvents =
                client
                        .getLogEvents(
                                GetLogEventsRequest.builder()
                                        .logGroupName(LOG_GROUP)
                                        .logStreamName(LOG_STREAM)
                                        .build())
                        .join();
        eventCount = logEvents.events().size();
      }

      logEvents =
              client
                      .getLogEvents(
                              GetLogEventsRequest.builder()
                                      .logGroupName(LOG_GROUP)
                                      .logStreamName(LOG_STREAM)
                                      .build())
                      .join();

      assertThat(logEvents.events()).hasSize(1);

      // Verify sync event
      assertThat(logEvents.events())
              .anySatisfy(
                      logEvent -> {
                        String message = logEvent.message();
                        assertThat(message).contains("test_table_sync");
                        assertThat(message).contains("AfterTableRefreshedEvent");
                      });
    } finally {
      // Clean up
      syncListener.shutdown();
      client.close();
    }
  }
}
