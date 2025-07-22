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
import java.util.Optional;
import org.apache.polaris.service.events.jsonEventListener.aws.cloudwatch.AwsCloudWatchConfiguration;

@StaticInitSafe
@ConfigMapping(prefix = "polaris.event-listener.aws-cloudwatch")
@ApplicationScoped
public interface QuarkusAwsCloudWatchConfiguration extends AwsCloudWatchConfiguration {

  @WithName("log-group")
  @WithDefault("polaris-cloudwatch-default-group")
  @Override
  Optional<String> awsCloudwatchlogGroup();

  @WithName("log-stream")
  @WithDefault("polaris-cloudwatch-default-stream")
  @Override
  Optional<String> awsCloudwatchlogStream();

  @WithName("region")
  @WithDefault("us-east-1")
  @Override
  Optional<String> awsCloudwatchRegion();

  @WithName("synchronous-mode")
  @WithDefault("false")
  @Override
  String synchronousMode();
}
