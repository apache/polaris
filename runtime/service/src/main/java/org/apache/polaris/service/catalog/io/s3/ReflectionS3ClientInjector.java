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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Map;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * Best-effort reflective injector that attempts to wire a pre-built S3Client into Iceberg's
 * S3FileIO instance. This avoids needing to modify Iceberg.
 */
public final class ReflectionS3ClientInjector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionS3ClientInjector.class);

  private ReflectionS3ClientInjector() {}

  /**
   * For Iceberg's S3FileIO implementation, PrefixedS3Client holds SerializableSupplier<S3Client>
   * fields named 's3' and 's3Async' which are used to lazily construct the real clients. To ensure
   * Iceberg will use our prebuilt S3Client (configured with the provided SdkHttpClient), replace
   * those supplier fields with suppliers that return the provided instances.
   */
  public static boolean injectSupplierIntoS3FileIO(
      Object s3FileIOInstance, S3Client prebuiltS3Client, S3AsyncClient prebuiltS3AsyncClient) {
    if (s3FileIOInstance == null) {
      return false;
    }

    Class<?> clazz = s3FileIOInstance.getClass();
    boolean changed = false;
    while (clazz != null) {
      try {
        Field s3Field = null;
        Field s3AsyncField = null;
        try {
          s3Field = clazz.getDeclaredField("s3");
        } catch (NoSuchFieldException ignored) {
        }
        try {
          s3AsyncField = clazz.getDeclaredField("s3Async");
        } catch (NoSuchFieldException ignored) {
        }

        if (s3Field != null) {
          s3Field.setAccessible(true);
          // Build a SerializableSupplier that returns our prebuilt S3Client.
          // Use a simple SerializableSupplier implementation used by Iceberg:
          // org.apache.iceberg.util.SerializableSupplier
          try {
            Class<?> serializableSupplierClazz =
                Class.forName("org.apache.iceberg.util.SerializableSupplier");
            Object serializableSupplier =
                java.lang.reflect.Proxy.newProxyInstance(
                    serializableSupplierClazz.getClassLoader(),
                    new Class<?>[] {serializableSupplierClazz, java.io.Serializable.class},
                    (proxy, method, args) -> {
                      if ("get".equals(method.getName())) {
                        return prebuiltS3Client;
                      }
                      // default proxy behavior
                      return method.invoke(proxy, args);
                    });

            s3Field.set(s3FileIOInstance, serializableSupplier);
            changed = true;
          } catch (ClassNotFoundException cnfe) {
            // Fallback: try to set any field assignable from java.util.function.Supplier
            Object simple = (java.util.function.Supplier<S3Client>) () -> prebuiltS3Client;
            s3Field.set(s3FileIOInstance, simple);
            changed = true;
          }
        }

        if (s3AsyncField != null && prebuiltS3AsyncClient != null) {
          s3AsyncField.setAccessible(true);
          try {
            Class<?> serializableSupplierClazz =
                Class.forName("org.apache.iceberg.util.SerializableSupplier");
            Object serializableSupplierAsync =
                java.lang.reflect.Proxy.newProxyInstance(
                    serializableSupplierClazz.getClassLoader(),
                    new Class<?>[] {serializableSupplierClazz, java.io.Serializable.class},
                    (proxy, method, args) -> {
                      if ("get".equals(method.getName())) {
                        return prebuiltS3AsyncClient;
                      }
                      return method.invoke(proxy, args);
                    });

            s3AsyncField.set(s3FileIOInstance, serializableSupplierAsync);
            changed = true;
          } catch (ClassNotFoundException cnfe) {
            Object simpleAsync =
                (java.util.function.Supplier<S3AsyncClient>) () -> prebuiltS3AsyncClient;
            s3AsyncField.set(s3FileIOInstance, simpleAsync);
            changed = true;
          }
        }
      } catch (Throwable t) {
        LOGGER.debug("Failed to set supplier fields on {}: {}", clazz, t.getMessage());
      }

      clazz = clazz.getSuperclass();
    }

    return changed;
  }

  public static S3Client buildS3Client(SdkHttpClient httpClient, Map<String, String> properties) {
    S3ClientBuilder builder = S3Client.builder();
    if (httpClient != null) {
      builder.httpClient(httpClient);
    }
    AwsCredentialsProvider creds = credentialsProviderFrom(properties);
    if (creds != null) builder.credentialsProvider(creds);

    applyRegionAndEndpoint(builder, properties);
    builder.serviceConfiguration(s3ConfigurationFrom(properties));

    return builder.build();
  }

  public static S3AsyncClient buildS3AsyncClient(
      SdkHttpClient httpClient, Map<String, String> properties) {
    try {
      // Attempt to build an async client. If the provided httpClient is an instance that also
      // implements the async HTTP client interface, use it. Otherwise fall back to the
      // default async client builder.
      software.amazon.awssdk.http.async.SdkAsyncHttpClient asyncHttpClient = null;
      if (httpClient instanceof software.amazon.awssdk.http.async.SdkAsyncHttpClient async) {
        asyncHttpClient = async;
      }

      software.amazon.awssdk.services.s3.S3AsyncClientBuilder asyncBuilder =
          software.amazon.awssdk.services.s3.S3AsyncClient.builder();

      if (asyncHttpClient != null) {
        asyncBuilder.httpClient(asyncHttpClient);
      }

      AwsCredentialsProvider creds = credentialsProviderFrom(properties);
      if (creds != null) asyncBuilder.credentialsProvider(creds);

      applyRegionAndEndpoint(asyncBuilder, properties);
      asyncBuilder.serviceConfiguration(s3ConfigurationFrom(properties));

      return asyncBuilder.build();
    } catch (Exception e) {
      LOGGER.debug("Failed to build S3AsyncClient: {}", e.toString());
      return null;
    }
  }

  private static AwsCredentialsProvider credentialsProviderFrom(Map<String, String> properties) {
    String accessKey = properties.get(StorageAccessProperty.AWS_KEY_ID.getPropertyName());
    String secretKey = properties.get(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName());
    if (accessKey != null && secretKey != null) {
      AwsBasicCredentials creds = AwsBasicCredentials.create(accessKey, secretKey);
      return StaticCredentialsProvider.create(creds);
    }
    return null;
  }

  private static void applyRegionAndEndpoint(Object builder, Map<String, String> properties) {
    String region = properties.get(StorageAccessProperty.CLIENT_REGION.getPropertyName());
    if (region != null) {
      try {
        Method m = builder.getClass().getMethod("region", Region.class);
        m.invoke(builder, Region.of(region));
      } catch (Exception ignored) {
        LOGGER.debug("Unable to apply region to builder {}", builder.getClass().getName());
      }
    }

    String endpoint = properties.get(StorageAccessProperty.AWS_ENDPOINT.getPropertyName());
    if (endpoint != null) {
      try {
        Method m = builder.getClass().getMethod("endpointOverride", URI.class);
        m.invoke(builder, URI.create(endpoint));
      } catch (Exception ignored) {
        LOGGER.debug(
            "Unable to apply endpointOverride to builder {}", builder.getClass().getName());
      }
    }
  }

  private static S3Configuration s3ConfigurationFrom(Map<String, String> properties) {
    String pathStyle =
        properties.get(StorageAccessProperty.AWS_PATH_STYLE_ACCESS.getPropertyName());
    boolean forcePathStyle = "true".equals(pathStyle);
    return S3Configuration.builder().pathStyleAccessEnabled(forcePathStyle).build();
  }
}
