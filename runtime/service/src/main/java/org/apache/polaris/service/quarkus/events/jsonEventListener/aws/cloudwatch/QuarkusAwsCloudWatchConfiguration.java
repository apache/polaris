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
package org.apache.polaris.service.quarkus.events.jsonEventListener.aws.cloudwatch;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.service.events.jsonEventListener.aws.cloudwatch.AwsCloudWatchConfiguration;

/**
 * Quarkus-specific configuration interface for AWS CloudWatch event listener integration.
 *
 * <p>This interface extends the base {@link AwsCloudWatchConfiguration} and provides
 * Quarkus-specific configuration mappings for AWS CloudWatch logging functionality.
 */
@StaticInitSafe
@ConfigMapping(prefix = "polaris.event-listener.aws-cloudwatch")
@ApplicationScoped
public interface QuarkusAwsCloudWatchConfiguration extends AwsCloudWatchConfiguration {

  /**
   * Returns the AWS CloudWatch log group name for event logging.
   *
   * <p>The log group is a collection of log streams that share the same retention, monitoring, and
   * access control settings. If not specified, defaults to "polaris-cloudwatch-default-group".
   *
   * <p>Configuration property: {@code polaris.event-listener.aws-cloudwatch.log-group}
   *
   * @return a String containing the log group name, or the default value if not configured
   */
  @WithName("log-group")
  @WithDefault("polaris-cloudwatch-default-group")
  @Override
  String awsCloudWatchLogGroup();

  /**
   * Returns the AWS CloudWatch log stream name for event logging.
   *
   * <p>A log stream is a sequence of log events that share the same source. Each log stream belongs
   * to one log group. If not specified, defaults to "polaris-cloudwatch-default-stream".
   *
   * <p>Configuration property: {@code polaris.event-listener.aws-cloudwatch.log-stream}
   *
   * @return a String containing the log stream name, or the default value if not configured
   */
  @WithName("log-stream")
  @WithDefault("polaris-cloudwatch-default-stream")
  @Override
  String awsCloudWatchLogStream();

  /**
   * Returns the AWS region where CloudWatch logs should be sent.
   *
   * <p>This specifies the AWS region for the CloudWatch service endpoint. The region must be a
   * valid AWS region identifier. If not specified, defaults to "us-east-1".
   *
   * <p>Configuration property: {@code polaris.event-listener.aws-cloudwatch.region}
   *
   * @return a String containing the AWS region, or the default value if not configured
   */
  @WithName("region")
  @WithDefault("us-east-1")
  @Override
  String awsCloudWatchRegion();

  /**
   * Returns the synchronous mode setting for CloudWatch logging.
   *
   * <p>When set to "true", log events are sent to CloudWatch synchronously, which may impact
   * application performance but ensures immediate delivery. When set to "false" (default), log
   * events are sent asynchronously for better performance.
   *
   * <p>Configuration property: {@code polaris.event-listener.aws-cloudwatch.synchronous-mode}
   *
   * @return a boolean value indicating the synchronous mode setting
   */
  @WithName("synchronous-mode")
  @WithDefault("false")
  @Override
  boolean synchronousMode();
}
