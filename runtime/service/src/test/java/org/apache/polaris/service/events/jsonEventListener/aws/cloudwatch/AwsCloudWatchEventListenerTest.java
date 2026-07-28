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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.configuration.MemorySize;
import java.math.BigInteger;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.config.PolarisIcebergObjectMapperCustomizer;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.test.floci.aws.FlociAws;
import org.apache.polaris.test.floci.aws.FlociAwsAccess;
import org.apache.polaris.test.floci.aws.FlociAwsExtension;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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

@ExtendWith(FlociAwsExtension.class)
class AwsCloudWatchEventListenerTest {
  private static final String REGION = "us-east-1";
  private static final String REALM = "test-realm";
  private static final String TEST_USER = "test-user";
  private static final PolarisPrincipal PRINCIPAL =
      PolarisPrincipal.of(TEST_USER, Map.of(), Set.of("role1", "role2"));
  private static final Clock CLOCK = Clock.systemUTC();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    new PolarisIcebergObjectMapperCustomizer(new MemorySize(BigInteger.valueOf(1024 * 1024)))
        .customize(OBJECT_MAPPER);
  }

  @Mock private AwsCloudWatchConfiguration config;

  @FlociAws(bucket = "cloudwatch-test-bucket", region = REGION)
  private static FlociAwsAccess flociAws;

  private AutoCloseable mockitoContext;
  private String logGroup;
  private String logStream;

  @BeforeEach
  void setUp() {
    mockitoContext = MockitoAnnotations.openMocks(this);
    logGroup = "test-log-group-" + UUID.randomUUID();
    logStream = "test-log-stream-" + UUID.randomUUID();

    // Configure the mocks
    when(config.awsCloudWatchLogGroup()).thenReturn(logGroup);
    when(config.awsCloudWatchLogStream()).thenReturn(logStream);
    when(config.awsCloudWatchRegion()).thenReturn(REGION);
    when(config.synchronousMode()).thenReturn(false); // Default to async mode
  }

  @AfterEach
  void tearDown() throws Exception {
    if (mockitoContext != null) {
      mockitoContext.close();
    }
  }

  private CloudWatchLogsAsyncClient createCloudWatchAsyncClient() {
    return CloudWatchLogsAsyncClient.builder()
        .endpointOverride(flociAws.endpoint())
        .credentialsProvider(flociAws.credentialsProvider())
        .region(Region.of(flociAws.region().orElse(REGION)))
        .build();
  }

  private AwsCloudWatchEventListener createListener(CloudWatchLogsAsyncClient client) {
    return new AwsCloudWatchEventListener(config, OBJECT_MAPPER, CLOCK) {
      @Override
      protected CloudWatchLogsAsyncClient createCloudWatchAsyncClient() {
        return client;
      }
    };
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
    client.createLogGroup(CreateLogGroupRequest.builder().logGroupName(logGroup).build()).join();
    client
        .createLogStream(
            CreateLogStreamRequest.builder()
                .logGroupName(logGroup)
                .logStreamName(logStream)
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
      listener.onEvent(
          new PolarisEvent(
              PolarisEventType.AFTER_REFRESH_TABLE,
              PolarisEventMetadata.builder().realmId(REALM).user(PRINCIPAL).build(),
              new EventAttributeMap()
                  .put(EventAttributes.CATALOG_NAME, "test_catalog")
                  .put(EventAttributes.TABLE_IDENTIFIER, testTable)));

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
                                .logGroupName(logGroup)
                                .logStreamName(logStream)
                                .build())
                        .join();
                assertThat(resp.events().size()).isGreaterThan(0);
              });
      GetLogEventsResponse logEvents =
          client
              .getLogEvents(
                  GetLogEventsRequest.builder()
                      .logGroupName(logGroup)
                      .logStreamName(logStream)
                      .build())
              .join();

      assertThat(logEvents.events())
          .hasSize(1)
          .first()
          .satisfies(
              logEvent -> {
                String message = logEvent.message();
                assertThat(message).contains(REALM);
                assertThat(message).contains(PolarisEventType.AFTER_REFRESH_TABLE.name());
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
      syncListener.onEvent(
          new PolarisEvent(
              PolarisEventType.AFTER_REFRESH_TABLE,
              PolarisEventMetadata.builder().realmId(REALM).user(PRINCIPAL).build(),
              new EventAttributeMap()
                  .put(EventAttributes.CATALOG_NAME, "test_catalog")
                  .put(EventAttributes.TABLE_IDENTIFIER, syncTestTable)));

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
                                .logGroupName(logGroup)
                                .logStreamName(logStream)
                                .build())
                        .join();
                assertThat(resp.events().size()).isGreaterThan(0);
              });

      GetLogEventsResponse logEvents =
          client
              .getLogEvents(
                  GetLogEventsRequest.builder()
                      .logGroupName(logGroup)
                      .logStreamName(logStream)
                      .build())
              .join();

      assertThat(logEvents.events()).hasSize(1);

      // Verify sync event
      assertThat(logEvents.events())
          .anySatisfy(
              logEvent -> {
                String message = logEvent.message();
                assertThat(message).contains("test_table_sync");
                assertThat(message).contains("AFTER_REFRESH_TABLE");
              });
    } finally {
      // Clean up
      syncListener.shutdown();
      client.close();
    }
  }

  private void verifyLogGroupAndStreamExist(CloudWatchLogsAsyncClient client) {
    // Verify log group exists
    DescribeLogGroupsResponse groups =
        client
            .describeLogGroups(
                DescribeLogGroupsRequest.builder().logGroupNamePrefix(logGroup).build())
            .join();
    assertThat(groups.logGroups())
        .hasSize(1)
        .first()
        .satisfies(group -> assertThat(group.logGroupName()).isEqualTo(logGroup));

    // Verify log stream exists
    DescribeLogStreamsResponse streams =
        client
            .describeLogStreams(
                DescribeLogStreamsRequest.builder()
                    .logGroupName(logGroup)
                    .logStreamNamePrefix(logStream)
                    .build())
            .join();
    assertThat(streams.logStreams())
        .hasSize(1)
        .first()
        .satisfies(stream -> assertThat(stream.logStreamName()).isEqualTo(logStream));
  }
}
