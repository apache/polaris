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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.events.AfterTableRefreshedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;

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

  enum TestMode {
    LOCALSTACK,
    MOCKED
  }

  // Test data provider
  static Stream<Arguments> testModeProvider() {
    return Stream.of(Arguments.of(TestMode.LOCALSTACK), Arguments.of(TestMode.MOCKED));
  }

  @BeforeEach
  void setUp() {
    mockitoContext = MockitoAnnotations.openMocks(this);
    executorService = Executors.newSingleThreadExecutor();

    // Configure the mocks
    when(config.awsCloudwatchlogGroup()).thenReturn(Optional.of(LOG_GROUP));
    when(config.awsCloudwatchlogStream()).thenReturn(Optional.of(LOG_STREAM));
    when(config.awsCloudwatchRegion()).thenReturn(Optional.of("us-east-1"));
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

  private CloudWatchLogsAsyncClient createCloudWatchAsyncClient(TestMode mode) {
    switch (mode) {
      case LOCALSTACK:
        if (!localStack.isRunning()) {
          localStack.start();
        }
        return CloudWatchLogsAsyncClient.builder()
            .endpointOverride(localStack.getEndpoint())
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localStack.getAccessKey(), localStack.getSecretKey())))
            .region(Region.of(localStack.getRegion()))
            .build();
      case MOCKED:
        CloudWatchLogsAsyncClient mockClient = Mockito.mock(CloudWatchLogsAsyncClient.class);

        // Mock the responses for log group and stream creation
        when(mockClient.createLogGroup(any(CreateLogGroupRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockClient.createLogStream(any(CreateLogStreamRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        // Mock the describe log streams response for getting sequence token
        DescribeLogStreamsResponse mockStreamsResponse =
            DescribeLogStreamsResponse.builder()
                .logStreams(
                    software.amazon.awssdk.services.cloudwatchlogs.model.LogStream.builder()
                        .logStreamName(LOG_STREAM)
                        .uploadSequenceToken(null)
                        .build())
                .build();
        when(mockClient.describeLogStreams(any(DescribeLogStreamsRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(mockStreamsResponse));

        // Mock the putLogEvents response
        PutLogEventsResponse mockPutResponse =
            PutLogEventsResponse.builder().nextSequenceToken("mock-sequence-token-123").build();
        when(mockClient.putLogEvents(any(PutLogEventsRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(mockPutResponse));

        return mockClient;
      default:
        throw new IllegalArgumentException("Unknown test mode: " + mode);
    }
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

  @ParameterizedTest
  @MethodSource("testModeProvider")
  void shouldCreateLogGroupAndStream(TestMode mode) {
    CloudWatchLogsAsyncClient client = createCloudWatchAsyncClient(mode);
    AwsCloudWatchEventListener listener = createListener(client);

    // Start the listener which should create the log group and stream
    listener.start();

    if (mode == TestMode.LOCALSTACK) {
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
    } else {
      // Verify method calls for mocked client
      verify(client, times(1))
          .createLogGroup(
              argThat((CreateLogGroupRequest request) -> request.logGroupName().equals(LOG_GROUP)));
      verify(client, times(1))
          .createLogStream(
              argThat(
                  (CreateLogStreamRequest request) ->
                      request.logGroupName().equals(LOG_GROUP)
                          && request.logStreamName().equals(LOG_STREAM)));
    }

    // Clean up
    listener.shutdown();
    if (client != null && mode == TestMode.LOCALSTACK) {
      client.close();
    }
  }

  @ParameterizedTest
  @MethodSource("testModeProvider")
  void shouldSendEventToCloudWatch(TestMode mode) {
    CloudWatchLogsAsyncClient client = createCloudWatchAsyncClient(mode);
    AwsCloudWatchEventListener listener = createListener(client);

    // Start the listener
    listener.start();

    // Create and send a test event
    TableIdentifier testTable = TableIdentifier.of("test_namespace", "test_table");
    AfterTableRefreshedEvent event = new AfterTableRefreshedEvent(testTable);
    listener.onAfterTableRefreshed(event);

    if (mode == TestMode.LOCALSTACK) {
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
    } else {
      // Verify that putLogEvents was called with the expected content
      verify(client, times(1))
          .putLogEvents(
              argThat(
                  (PutLogEventsRequest request) -> {
                    // Verify basic request structure
                    assertThat(request.logGroupName()).isEqualTo(LOG_GROUP);
                    assertThat(request.logStreamName()).isEqualTo(LOG_STREAM);
                    assertThat(request.logEvents()).hasSize(1);

                    // Verify the log event content
                    String logMessage = request.logEvents().getFirst().message();
                    assertThat(logMessage).contains(REALM);
                    assertThat(logMessage).contains(TEST_USER);
                    assertThat(logMessage).contains("AfterTableRefreshedEvent");
                    assertThat(logMessage).contains(testTable.toString());

                    return true;
                  }));
    }

    // Clean up
    listener.shutdown();
    if (client != null && mode == TestMode.LOCALSTACK) {
      client.close();
    }
  }

  @ParameterizedTest
  @MethodSource("testModeProvider")
  void handleSynchronousModeCorrectly(TestMode mode) {
    CloudWatchLogsAsyncClient client = createCloudWatchAsyncClient(mode);

    // Test synchronous mode
    when(config.synchronousMode()).thenReturn(true);
    AwsCloudWatchEventListener syncListener = createListener(client);
    syncListener.start();

    // Create and send a test event synchronously
    TableIdentifier syncTestTable = TableIdentifier.of("test_namespace", "test_table_sync");
    AfterTableRefreshedEvent syncEvent = new AfterTableRefreshedEvent(syncTestTable);
    syncListener.onAfterTableRefreshed(syncEvent);

    if (mode == TestMode.LOCALSTACK) {
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
    } else {
      // Verify that putLogEvents was called with the expected content
      verify(client, times(1))
          .putLogEvents(
              argThat(
                  (PutLogEventsRequest request) -> {
                    // Verify basic request structure
                    assertThat(request.logGroupName()).isEqualTo(LOG_GROUP);
                    assertThat(request.logStreamName()).isEqualTo(LOG_STREAM);
                    assertThat(request.logEvents()).hasSize(1);

                    // Verify the log event content
                    String logMessage = request.logEvents().getFirst().message();
                    assertThat(logMessage).contains(REALM);
                    assertThat(logMessage).contains(TEST_USER);
                    assertThat(logMessage).contains("AfterTableRefreshedEvent");
                    assertThat(logMessage).contains("test_table_sync");

                    return true;
                  }));
    }

    // Clean up
    syncListener.shutdown();
    if (client != null && mode == TestMode.LOCALSTACK) {
      client.close();
    }
  }
}
