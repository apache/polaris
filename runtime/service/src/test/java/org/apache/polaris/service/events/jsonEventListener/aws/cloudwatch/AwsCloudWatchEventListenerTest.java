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
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import io.quarkus.runtime.configuration.MemorySize;
import jakarta.ws.rs.core.SecurityContext;
import java.math.BigInteger;
import java.security.Principal;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.config.PolarisIcebergObjectMapperCustomizer;
import org.apache.polaris.service.events.AfterTableRefreshedEvent;
import org.awaitility.Awaitility;
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
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
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
  private static final BigInteger MAX_BODY_SIZE = BigInteger.valueOf(1024 * 1024);
  private static final PolarisIcebergObjectMapperCustomizer customizer =
      new PolarisIcebergObjectMapperCustomizer(new MemorySize(MAX_BODY_SIZE));

  @Mock private AwsCloudWatchConfiguration config;

  private ExecutorService executorService;
  private AutoCloseable mockitoContext;

  @BeforeEach
  void setUp() {
    mockitoContext = MockitoAnnotations.openMocks(this);
    executorService = Executors.newSingleThreadExecutor();

    // Configure the mocks
    when(config.awsCloudWatchLogGroup()).thenReturn(LOG_GROUP);
    when(config.awsCloudWatchLogStream()).thenReturn(LOG_STREAM);
    when(config.awsCloudWatchRegion()).thenReturn("us-east-1");
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
        new AwsCloudWatchEventListener(config, clock, customizer) {
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
    Principal principal = Mockito.mock(PolarisPrincipal.class);
    when(callContext.getRealmContext()).thenReturn(realmContext);
    when(callContext.getPolarisCallContext()).thenReturn(polarisCallContext);
    when(realmContext.getRealmIdentifier()).thenReturn(REALM);
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(principal.getName()).thenReturn(TEST_USER);
    when(((PolarisPrincipal) principal).getRoles()).thenReturn(Set.of("role1", "role2"));
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
      verifyLogGroupAndStreamExist(client);
    } finally {
      client.close();
      listener.shutdown();
    }
  }

  @Test
  void shouldAcceptPreviouslyCreatedLogGroupAndStream() {
    CloudWatchLogsAsyncClient client = createCloudWatchAsyncClient();
    client.createLogGroup(CreateLogGroupRequest.builder().logGroupName(LOG_GROUP).build()).join();
    client
        .createLogStream(
            CreateLogStreamRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamName(LOG_STREAM)
                .build())
        .join();
    verifyLogGroupAndStreamExist(client);

    AwsCloudWatchEventListener listener = createListener(client);
    listener.start();
    try {
      verifyLogGroupAndStreamExist(client);
    } finally {
      client.close();
      listener.shutdown();
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
      AfterTableRefreshedEvent event = new AfterTableRefreshedEvent("test_catalog", testTable);
      listener.onAfterTableRefreshed(event);

      Awaitility.await("expected amount of records should be sent to CloudWatch")
          .atMost(Duration.ofSeconds(30))
          .pollDelay(Duration.ofMillis(100))
          .pollInterval(Duration.ofMillis(100))
          .untilAsserted(
              () -> {
                GetLogEventsResponse resp =
                    client
                        .getLogEvents(
                            GetLogEventsRequest.builder()
                                .logGroupName(LOG_GROUP)
                                .logStreamName(LOG_STREAM)
                                .build())
                        .join();
                assertThat(resp.events().size()).isGreaterThan(0);
              });
      GetLogEventsResponse logEvents =
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
      AfterTableRefreshedEvent syncEvent = new AfterTableRefreshedEvent("test_catalog", syncTestTable);
      syncListener.onAfterTableRefreshed(syncEvent);

      Awaitility.await("expected amount of records should be sent to CloudWatch")
          .atMost(Duration.ofSeconds(30))
          .pollDelay(Duration.ofMillis(100))
          .pollInterval(Duration.ofMillis(100))
          .untilAsserted(
              () -> {
                GetLogEventsResponse resp =
                    client
                        .getLogEvents(
                            GetLogEventsRequest.builder()
                                .logGroupName(LOG_GROUP)
                                .logStreamName(LOG_STREAM)
                                .build())
                        .join();
                assertThat(resp.events().size()).isGreaterThan(0);
              });

      GetLogEventsResponse logEvents =
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

  @Test
  void ensureObjectMapperCustomizerIsApplied() {
    AwsCloudWatchEventListener listener = createListener(createCloudWatchAsyncClient());
    listener.start();

    assertThat(listener.objectMapper.getPropertyNamingStrategy())
        .isInstanceOf(PropertyNamingStrategies.KebabCaseStrategy.class);
    assertThat(listener.objectMapper.getFactory().streamReadConstraints().getMaxDocumentLength())
        .isEqualTo(MAX_BODY_SIZE.longValue());
  }

  private void verifyLogGroupAndStreamExist(CloudWatchLogsAsyncClient client) {
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
  }
}
