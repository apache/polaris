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
package org.apache.polaris.service.storage;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.jspecify.annotations.Nullable;

/**
 * A third {@link S3CredentialVendingMechanism}, installed only under {@link ThirdMechanismProfile},
 * that records every call it receives and vends a fixed, fake AWS credential triple. Proves that
 * the registry and the gates work for a mechanism the server itself does not ship, not only for
 * {@code STS}.
 */
@ApplicationScoped
@Alternative
@Identifier(TestS3CredentialVendingMechanism.ID)
public class TestS3CredentialVendingMechanism implements S3CredentialVendingMechanism {

  public static final String ID = "TEST_MECHANISM";
  public static final String FAKE_KEY = "TEST_MECHANISM_FAKE_KEY";
  public static final String FAKE_SECRET = "TEST_MECHANISM_FAKE_SECRET";
  public static final String FAKE_TOKEN = "TEST_MECHANISM_FAKE_TOKEN";

  /** One call: the storage config {@link #integrationFor} was given. */
  public record Call(AwsStorageConfigurationInfo storageConfig) {}

  private final List<Call> calls = new CopyOnWriteArrayList<>();

  public List<Call> calls() {
    return List.copyOf(calls);
  }

  /** One validation: the stored config, null at create, and the config being written. */
  public record Validation(
      @Nullable AwsStorageConfigurationInfo current, AwsStorageConfigurationInfo updated) {}

  private final List<Validation> validations = new CopyOnWriteArrayList<>();

  public List<Validation> validations() {
    return List.copyOf(validations);
  }

  public void clear() {
    calls.clear();
    validations.clear();
  }

  @Override
  public PolarisStorageIntegration integrationFor(AwsStorageConfigurationInfo storageConfig) {
    calls.add(new Call(storageConfig));
    return (grants, refreshEndpoint, context) ->
        StorageAccessConfig.builder()
            .putCredential(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), FAKE_KEY)
            .putCredential(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), FAKE_SECRET)
            .putCredential(StorageAccessProperty.AWS_TOKEN.getPropertyName(), FAKE_TOKEN)
            .build();
  }

  @Override
  public void validate(
      @Nullable AwsStorageConfigurationInfo current, AwsStorageConfigurationInfo updated) {
    validations.add(new Validation(current, updated));
    for (String location : updated.getAllowedLocations()) {
      if (location.contains("/refused/")) {
        throw new IllegalArgumentException(
            "TEST_MECHANISM refuses the allowed location " + location);
      }
    }
  }
}
