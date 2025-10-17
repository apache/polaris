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

package org.apache.polaris.service.catalog.io.s3;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/** Unit tests for ReflectionS3ClientInjector. */
public class ReflectionS3ClientInjectorTest {

  private static class DummyS3FileIO {
    // Simulate Iceberg's SerializableSupplier<S3Client> and SerializableSupplier<S3AsyncClient>
    public Supplier<S3Client> s3;
    public Supplier<S3AsyncClient> s3Async;
  }

  @Test
  public void testInjectSupplierIntoS3FileIO_replacesSuppliers() {
    DummyS3FileIO fileIO = new DummyS3FileIO();

    // Build simple clients with default builders (they won't be used to actually call network)
    S3Client prebuilt = S3Client.builder().build();
    S3AsyncClient prebuiltAsync = S3AsyncClient.builder().build();

    boolean injected =
        ReflectionS3ClientInjector.injectSupplierIntoS3FileIO(fileIO, prebuilt, prebuiltAsync);
    assertTrue(injected, "Expected supplier injection to return true");

    // The injected suppliers should return the prebuilt instances
    Supplier<S3Client> s3Supplier = fileIO.s3;
    Supplier<S3AsyncClient> s3AsyncSupplier = fileIO.s3Async;

    assertSame(prebuilt, s3Supplier.get());
    assertSame(prebuiltAsync, s3AsyncSupplier.get());
  }
}
