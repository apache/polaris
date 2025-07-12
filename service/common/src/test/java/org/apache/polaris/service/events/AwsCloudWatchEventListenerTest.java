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

package org.apache.polaris.service.events.listeners;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.events.AfterCatalogCreatedEvent;
import org.apache.polaris.service.events.AwsCloudWatchEventListener;
import org.apache.polaris.service.events.EventListenerConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsCloudWatchEventListenerTest.class);

    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:3.4");

    private static final LocalStackContainer localStack =
            new LocalStackContainer(LOCALSTACK_IMAGE)
                    .withServices(LocalStackContainer.Service.CLOUDWATCHLOGS);

    private static final String LOG_GROUP = "test-log-group";
    private static final String LOG_STREAM = "test-log-stream";
    private static final String REALM = "test-realm";

    @Mock
    private EventListenerConfiguration config;

    @Mock
    private CallContext callContext;

    @Mock
    private RealmContext realmContext;

    @Mock
    private PolarisCallContext polarisCallContext;

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
        when(callContext.getRealmContext()).thenReturn(realmContext);
        when(callContext.getPolarisCallContext()).thenReturn(polarisCallContext);
        when(polarisCallContext.getClock()).thenReturn(Clock.systemUTC());
        when(realmContext.getRealmIdentifier()).thenReturn(REALM);

        // Create CloudWatch client pointing to LocalStack
        cloudWatchLogsClient = CloudWatchLogsClient.builder()
                .endpointOverride(localStack.getEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey())))
                .region(Region.of(localStack.getRegion()))
                .build();

        listener = new AwsCloudWatchEventListener(config, executorService) {
            @Override
            protected CloudWatchLogsClient createCloudWatchClient() {
                return cloudWatchLogsClient;
            }
        };
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
        DescribeLogGroupsResponse groups = cloudWatchLogsClient.describeLogGroups(DescribeLogGroupsRequest.builder()
                .logGroupNamePrefix(LOG_GROUP)
                .build());
        assertThat(groups.logGroups())
                .hasSize(1)
                .first()
                .satisfies(group -> assertThat(group.logGroupName()).isEqualTo(LOG_GROUP));

        // Verify log stream exists
        DescribeLogStreamsResponse streams = cloudWatchLogsClient.describeLogStreams(DescribeLogStreamsRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamNamePrefix(LOG_STREAM)
                .build());
        assertThat(streams.logStreams())
                .hasSize(1)
                .first()
                .satisfies(stream -> assertThat(stream.logStreamName()).isEqualTo(LOG_STREAM));
    }

    @Test
    void shouldSendEventToCloudWatchSingleEventSubmissions() {
        listener.start();

        // Create a test catalog entity
        String catalog1Name = "test-catalog1";
        String catalog2Name = "test-catalog2";

        // Create and send the event
        AfterCatalogCreatedEvent event = new AfterCatalogCreatedEvent(getTestCatalog(catalog1Name));
        listener.onAfterCatalogCreated(event, callContext);

        // Wait a bit for the background thread to process
        listener.drainQueue();

        // Verify the event was sent to CloudWatch
        GetLogEventsResponse logEvents = cloudWatchLogsClient.getLogEvents(GetLogEventsRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamName(LOG_STREAM)
                .build());

        assertThat(logEvents.events())
                .hasSize(1)
                .first()
                .satisfies(logEvent -> {
                    String message = logEvent.message();
                    assertThat(message).contains(catalog1Name);
                    assertThat(message).contains(REALM);
                    assertThat(message).contains(AfterCatalogCreatedEvent.class.getSimpleName());
                });

        // Redo above procedure to ensure that non-cold-start events can also be submitted
        event = new AfterCatalogCreatedEvent(getTestCatalog(catalog2Name));
        listener.onAfterCatalogCreated(event, callContext);

        // Wait a bit for the background thread to process
        listener.drainQueue();

        // Verify the event was sent to CloudWatch
        logEvents = cloudWatchLogsClient.getLogEvents(GetLogEventsRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamName(LOG_STREAM)
                .build());

        assertThat(logEvents.events()).hasSize(2);
        String secondMsg = logEvents.events().get(1).message();
        assertThat(secondMsg).contains(catalog2Name);
        assertThat(secondMsg).contains(REALM);
        assertThat(secondMsg).contains(AfterCatalogCreatedEvent.class.getSimpleName());
    }

    @Test
    void shouldSendEventToCloudWatchBatchEventSubmissions() {
        listener.start();
        String catalog1Name = "test-catalog1";
        String catalog2Name = "test-catalog2";

        AfterCatalogCreatedEvent event1 = new AfterCatalogCreatedEvent(getTestCatalog(catalog1Name));
        AfterCatalogCreatedEvent event2 = new AfterCatalogCreatedEvent(getTestCatalog(catalog2Name));

        listener.onAfterCatalogCreated(event1, callContext);
        listener.onAfterCatalogCreated(event2, callContext);
        listener.drainQueue();

        // Verify the events were sent to CloudWatch
        GetLogEventsResponse logEvents = cloudWatchLogsClient.getLogEvents(GetLogEventsRequest.builder()
                .logGroupName(LOG_GROUP)
                .logStreamName(LOG_STREAM)
                .build());

        assertThat(logEvents.events()).hasSize(2);
        List<OutputLogEvent> sortedEvents = new ArrayList<>(logEvents.events());
        sortedEvents.sort(Comparator.comparingLong(OutputLogEvent::timestamp));
        assertThat(sortedEvents.get(0).message()).contains(catalog1Name);
        assertThat(sortedEvents.get(1).message()).contains(catalog2Name);
    }

    private Catalog getTestCatalog(String catalogName) {
        return PolarisCatalog.builder()
                .setName(catalogName)
                .setType(Catalog.TypeEnum.INTERNAL)
                .setStorageConfigInfo(FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
                        .setAllowedLocations(List.of("file://"))
                        .build())
                .build();
    }
}
