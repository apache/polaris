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

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;

class AwsCloudWatchEventListenerTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AwsCloudWatchEventListenerTest.class);

  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:3.4");

  private static final LocalStackContainer localStack =
      new LocalStackContainer(LOCALSTACK_IMAGE)
          .withServices(LocalStackContainer.Service.CLOUDWATCHLOGS);

  private static final String LOG_GROUP = "test-log-group";
  private static final String LOG_STREAM = "test-log-stream";
  private static final String REALM = "test-realm";
  private static final String TEST_USER = "test-user";

  @Mock private EventListenerConfiguration config;

  private ExecutorService executorService;
  private AwsCloudWatchEventListener listener;
  private CloudWatchLogsClient cloudWatchLogsClient;
  private AutoCloseable mockitoContext;

  @BeforeEach
  void setUp() {
    localStack.start();
    mockitoContext = MockitoAnnotations.openMocks(this);
    executorService = Executors.newSingleThreadExecutor();

    // Configure the mocks
    when(config.awsCloudwatchlogGroup()).thenReturn(Optional.of(LOG_GROUP));
    when(config.awsCloudwatchlogStream()).thenReturn(Optional.of(LOG_STREAM));
    when(config.awsCloudwatchRegion()).thenReturn(Optional.of("us-east-1"));

    // Create CloudWatch client pointing to LocalStack
    cloudWatchLogsClient =
        CloudWatchLogsClient.builder()
            .endpointOverride(localStack.getEndpoint())
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localStack.getAccessKey(), localStack.getSecretKey())))
            .region(Region.of(localStack.getRegion()))
            .build();

    listener =
        new AwsCloudWatchEventListener(config, executorService) {
          @Override
          protected CloudWatchLogsClient createCloudWatchClient() {
            return cloudWatchLogsClient;
          }
        };

    CallContext callContext = Mockito.mock(CallContext.class);
    PolarisCallContext polarisCallContext = Mockito.mock(PolarisCallContext.class);
    RealmContext realmContext = Mockito.mock(RealmContext.class);
    SecurityContext securityContext = Mockito.mock(SecurityContext.class);
    Principal principal = Mockito.mock(Principal.class);
    when(callContext.getRealmContext()).thenReturn(realmContext);
    when(callContext.getPolarisCallContext()).thenReturn(polarisCallContext);
    when(polarisCallContext.getClock()).thenReturn(Clock.systemUTC());
    when(realmContext.getRealmIdentifier()).thenReturn(REALM);
    when(securityContext.getUserPrincipal()).thenReturn(principal);
    when(principal.getName()).thenReturn(TEST_USER);
    listener.callContext = callContext;
    listener.securityContext = securityContext;
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
    if (cloudWatchLogsClient != null) {
      cloudWatchLogsClient.close();
    }
    localStack.stop();
  }

  @Test
  void shouldCreateLogGroupAndStream() {
    // Start the listener which should create the log group and stream
    listener.start();

    // Verify log group exists
    DescribeLogGroupsResponse groups =
        cloudWatchLogsClient.describeLogGroups(
            DescribeLogGroupsRequest.builder().logGroupNamePrefix(LOG_GROUP).build());
    assertThat(groups.logGroups())
        .hasSize(1)
        .first()
        .satisfies(group -> assertThat(group.logGroupName()).isEqualTo(LOG_GROUP));

    // Verify log stream exists
    DescribeLogStreamsResponse streams =
        cloudWatchLogsClient.describeLogStreams(
            DescribeLogStreamsRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamNamePrefix(LOG_STREAM)
                .build());
    assertThat(streams.logStreams())
        .hasSize(1)
        .first()
        .satisfies(stream -> assertThat(stream.logStreamName()).isEqualTo(LOG_STREAM));
  }

  @Test
  void shouldSendEventToCloudWatch() {
    listener.start();

    // Create test table identifiers
    TableIdentifier t1 = TableIdentifier.of("test_ns", "test_table1");
    TableIdentifier t2 = TableIdentifier.of("test_ns", "test_table2");

    // Create and send the event
    AfterTableRefreshedEvent event = new AfterTableRefreshedEvent(t1);
    listener.onAfterTableRefreshed(event);

    // Verify future and status
    List<Future<?>> allActiveFutures = listener.getFutures().keySet().stream().toList();
    assertThat(allActiveFutures).hasSize(1);
    Future<?> firstFuture = allActiveFutures.get(0);

    // refactor method
    try {
      long start = System.currentTimeMillis();
      while (!firstFuture.isDone()) {
        if (System.currentTimeMillis() - start > 30 * 1000) {
          fail("Future interrupted or did not finish");
        }
        sleep(1000); // wait one second before checking again
      }
    } catch (InterruptedException e) {
      fail("Future interrupted or did not finish");
    }

    assertThat(firstFuture.isDone()).isTrue();
    assertThat(firstFuture.state()).isEqualTo(Future.State.SUCCESS);

    // Verify the event was sent to CloudWatch
    GetLogEventsResponse logEvents =
        cloudWatchLogsClient.getLogEvents(
            GetLogEventsRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamName(LOG_STREAM)
                .build());

    assertThat(logEvents.events())
        .hasSize(1)
        .first()
        .satisfies(
            logEvent -> {
              String message = logEvent.message();
              assertThat(message).contains(REALM);
              assertThat(message).contains(AfterTableRefreshedEvent.class.getSimpleName());
              assertThat(message).contains(TEST_USER);
              assertThat(message).contains(t1.toString());
            });

    // Redo above procedure to ensure that non-cold-start events can also be submitted
    event = new AfterTableRefreshedEvent(t2);
    listener.onAfterTableRefreshed(event);

    allActiveFutures = listener.getFutures().keySet().stream().toList();
    assertThat(allActiveFutures).hasSize(2);
    Future<?> secondFuture =
        allActiveFutures.stream().filter((f) -> !f.equals(firstFuture)).findFirst().get();

    try {
      long start = System.currentTimeMillis();
      while (!secondFuture.isDone()) {
        if (System.currentTimeMillis() - start > 30 * 1000) {
          fail("Future interrupted or did not finish");
        }
        sleep(1000); // wait one second before checking again
      }
    } catch (InterruptedException e) {
      fail("Future interrupted or did not finish");
    }

    assertThat(secondFuture.isDone()).isTrue();
    assertThat(secondFuture.state()).isEqualTo(Future.State.SUCCESS);
    assertThat(listener.getFutures().get(firstFuture)).isEqualTo(null); // first future got purged

    // Verify the event was sent to CloudWatch
    logEvents =
        cloudWatchLogsClient.getLogEvents(
            GetLogEventsRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamName(LOG_STREAM)
                .build());

    assertThat(logEvents.events()).hasSize(2);
    List<OutputLogEvent> sortedEvents = new ArrayList<>(logEvents.events());
    sortedEvents.sort(Comparator.comparingLong(OutputLogEvent::timestamp));
    String secondMsg = sortedEvents.get(1).message();
    assertThat(secondMsg).contains(REALM);
    assertThat(secondMsg).contains(AfterTableRefreshedEvent.class.getSimpleName());
    assertThat(secondMsg).contains(TEST_USER);
    assertThat(secondMsg).contains(t2.toString());
  }
}
