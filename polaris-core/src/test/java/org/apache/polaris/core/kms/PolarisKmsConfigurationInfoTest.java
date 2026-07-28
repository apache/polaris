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

package org.apache.polaris.core.kms;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.util.stream.Stream;
import org.apache.polaris.core.kms.aws.AwsKmsConfigurationInfo;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class PolarisKmsConfigurationInfoTest {
  @InjectSoftAssertions protected SoftAssertions soft;

  private static ObjectMapper mapper;

  @BeforeAll
  public static void setup() {
    mapper = JsonMapper.builder().build();
  }

  @ParameterizedTest
  @MethodSource
  public void reserialization(PolarisKmsConfigurationInfo configInfo, String serialized)
      throws Exception {
    var jsonStr = configInfo.serialize();

    var asJsonNode = mapper.readValue(jsonStr, JsonNode.class);
    var serializedJsonNode = mapper.readValue(serialized, JsonNode.class);
    soft.assertThat(asJsonNode).isEqualTo(serializedJsonNode);

    var deserialized = mapper.readValue(serialized, PolarisKmsConfigurationInfo.class);
    soft.assertThat(deserialized).isEqualTo(configInfo);

    var reserialized = mapper.readValue(jsonStr, PolarisKmsConfigurationInfo.class);
    soft.assertThat(reserialized).isEqualTo(configInfo);
  }

  static Stream<Arguments> reserialization() {
    return Stream.of(
        arguments(
            AwsKmsConfigurationInfo.builder()
                .kmsName("aws-kms")
                .roleArn("arn:aws:iam::123456789012:role/polaris-kms")
                .region("us-east-2")
                .endpoint("https://kms.us-east-2.amazonaws.com")
                .stsEndpoint("https://sts.us-east-2.amazonaws.com")
                .addAllowedKeyArn("arn:aws:kms:us-east-2:123456789012:key/read-key")
                .build(),
            "{\"@type\":\"AwsKmsConfigurationInfo\",\"kmsName\":\"aws-kms\",\"kmsType\":\"AWS\",\"roleArn\":\"arn:aws:iam::123456789012:role/polaris-kms\",\"region\":\"us-east-2\",\"endpoint\":\"https://kms.us-east-2.amazonaws.com\",\"stsEndpoint\":\"https://sts.us-east-2.amazonaws.com\",\"allowedKeyArns\":[\"arn:aws:kms:us-east-2:123456789012:key/read-key\"]}"));
  }
}
