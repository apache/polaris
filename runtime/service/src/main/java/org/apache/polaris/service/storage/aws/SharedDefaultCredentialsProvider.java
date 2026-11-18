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
package org.apache.polaris.service.storage.aws;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

/**
 * An {@link AwsCredentialsProvider} for Iceberg's {@code client.credentials-provider} property that
 * delegates to one {@link DefaultCredentialsProvider} shared by the whole server.
 *
 * <p>Without it, Iceberg's {@code S3FileIO} builds a new {@link DefaultCredentialsProvider} for
 * each S3 client when no credentials are configured, and each new provider fetches the credentials
 * again from their source (IMDS, STS web identity, ...). Iceberg instantiates this class through
 * its static {@link #create()} method, so the shared provider is held in a static field.
 *
 * <p>Instances do not implement {@code SdkAutoCloseable}, so closing an S3 client or a {@code
 * FileIO} never closes the shared provider.
 */
public final class SharedDefaultCredentialsProvider implements AwsCredentialsProvider {

  private static final class Holder {
    private static final DefaultCredentialsProvider SHARED =
        DefaultCredentialsProvider.builder().build();
  }

  private SharedDefaultCredentialsProvider() {}

  /** Called by Iceberg's {@code AwsClientProperties} when it loads this class by name. */
  public static SharedDefaultCredentialsProvider create() {
    return new SharedDefaultCredentialsProvider();
  }

  @Override
  public AwsCredentials resolveCredentials() {
    return Holder.SHARED.resolveCredentials();
  }
}
