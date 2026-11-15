---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: smallrye-polaris_event_listener_aws_cloudwatch
build:
  list: never
  render: never
---

Configuration interface for AWS CloudWatch event listener integration.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.event-listener.aws-cloudwatch.log-group` | `polaris-cloudwatch-default-group` | `string` | Returns the AWS CloudWatch log group name for event logging. <br><br>The log group is a collection of log streams that share the same retention, monitoring, and  access control settings. If not specified, defaults to "polaris-cloudwatch-default-group".   <br><br>Configuration property: `polaris.event-listener.aws-cloudwatch.log-group` |
| `polaris.event-listener.aws-cloudwatch.log-stream` | `polaris-cloudwatch-default-stream` | `string` | Returns the AWS CloudWatch log stream name for event logging. <br><br>A log stream is a sequence of log events that share the same source. Each log stream belongs  to one log group. If not specified, defaults to "polaris-cloudwatch-default-stream".   <br><br>Configuration property: `polaris.event-listener.aws-cloudwatch.log-stream` |
| `polaris.event-listener.aws-cloudwatch.region` | `us-east-1` | `string` | Returns the AWS region where CloudWatch logs should be sent. <br><br>This specifies the AWS region for the CloudWatch service endpoint. The region must be a  valid AWS region identifier. If not specified, defaults to "us-east-1".   <br><br>Configuration property: `polaris.event-listener.aws-cloudwatch.region` |
| `polaris.event-listener.aws-cloudwatch.synchronous-mode` | `false` | `boolean` | Returns the synchronous mode setting for CloudWatch logging. <br><br>When set to "true", log events are sent to CloudWatch synchronously, which may impact  application performance but ensures immediate delivery. When set to "false" (default), log  events are sent asynchronously for better performance.   <br><br>Configuration property: `polaris.event-listener.aws-cloudwatch.synchronous-mode` |
