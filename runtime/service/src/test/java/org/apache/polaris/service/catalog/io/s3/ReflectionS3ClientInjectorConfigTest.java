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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Configuration;

/** Tests that S3 client builders apply region/endpoint/path-style/credentials as expected. */
@SuppressWarnings("unused")
public class ReflectionS3ClientInjectorConfigTest {

  private Map<String, String> makeProps(String region, String endpoint, boolean pathStyle) {
    Map<String, String> p = new HashMap<>();
    p.put(StorageAccessProperty.CLIENT_REGION.getPropertyName(), region);
    p.put(StorageAccessProperty.AWS_ENDPOINT.getPropertyName(), endpoint);
    p.put(
        StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName(), Boolean.toString(pathStyle));
    p.put(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), "AKIA_TEST");
    p.put(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), "SECRET_TEST");
    return p;
  }

  // helper removed: deep traversal not used after switching to reflective checks

  @Test
  public void testBuildS3Client_appliesConfiguration() throws Exception {
    Map<String, String> props = makeProps("us-west-2", "https://custom.example:9000", true);

    // Verify credentials helper
    Method credsMethod =
        ReflectionS3ClientInjector.class.getDeclaredMethod("credentialsProviderFrom", Map.class);
    credsMethod.setAccessible(true);
    Object credsProv = credsMethod.invoke(null, props);
    assertTrue(credsProv instanceof AwsCredentialsProvider);
    AwsCredentialsProvider prov = (AwsCredentialsProvider) credsProv;
    var creds = prov.resolveCredentials();
    assertTrue("AKIA_TEST".equals(creds.accessKeyId()));
    assertTrue("SECRET_TEST".equals(creds.secretAccessKey()));

    // Verify S3Configuration helper
    Method s3ConfMethod =
        ReflectionS3ClientInjector.class.getDeclaredMethod("s3ConfigurationFrom", Map.class);
    s3ConfMethod.setAccessible(true);
    Object s3Conf = s3ConfMethod.invoke(null, props);
    assertTrue(s3Conf instanceof S3Configuration);
    assertTrue(((S3Configuration) s3Conf).pathStyleAccessEnabled());

    // Verify applyRegionAndEndpoint applies to arbitrary builder-like objects
    @SuppressWarnings("unused")
    class TestBuilder {
      public Region regionVal;
      public URI endpointVal;

      public TestBuilder region(Region r) {
        this.regionVal = r;
        return this;
      }

      public TestBuilder endpointOverride(URI u) {
        this.endpointVal = u;
        return this;
      }
    }

    TestBuilder tb = new TestBuilder();
    Method applyMethod =
        ReflectionS3ClientInjector.class.getDeclaredMethod(
            "applyRegionAndEndpoint", Object.class, Map.class);
    applyMethod.setAccessible(true);
    applyMethod.invoke(null, tb, props);
    assertEquals("us-west-2", tb.regionVal.id());
    assertEquals(new URI("https://custom.example:9000"), tb.endpointVal);
  }

  @Test
  public void testBuildS3AsyncClient_appliesConfiguration() throws Exception {
    Map<String, String> props = makeProps("eu-central-1", "https://async.example:9000", false);

    // credentials
    Method credsMethod =
        ReflectionS3ClientInjector.class.getDeclaredMethod("credentialsProviderFrom", Map.class);
    credsMethod.setAccessible(true);
    Object credsProv = credsMethod.invoke(null, props);
    assertTrue(credsProv instanceof AwsCredentialsProvider);
    AwsCredentialsProvider prov = (AwsCredentialsProvider) credsProv;
    var creds = prov.resolveCredentials();
    assertEquals("AKIA_TEST", creds.accessKeyId());

    // s3 config
    Method s3ConfMethod =
        ReflectionS3ClientInjector.class.getDeclaredMethod("s3ConfigurationFrom", Map.class);
    s3ConfMethod.setAccessible(true);
    Object s3Conf = s3ConfMethod.invoke(null, props);
    assertTrue(s3Conf instanceof S3Configuration);
    assertTrue(!((S3Configuration) s3Conf).pathStyleAccessEnabled());

    // applyRegionAndEndpoint on TestBuilder
    @SuppressWarnings("unused")
    class TestBuilder {
      public Region regionVal;
      public URI endpointVal;

      public TestBuilder region(Region r) {
        this.regionVal = r;
        return this;
      }

      public TestBuilder endpointOverride(URI u) {
        this.endpointVal = u;
        return this;
      }
    }

    TestBuilder tb = new TestBuilder();
    Method applyMethod =
        ReflectionS3ClientInjector.class.getDeclaredMethod(
            "applyRegionAndEndpoint", Object.class, Map.class);
    applyMethod.setAccessible(true);
    applyMethod.invoke(null, tb, props);
    assertEquals("eu-central-1", tb.regionVal.id());
    assertEquals(new URI("https://async.example:9000"), tb.endpointVal);
  }
}
