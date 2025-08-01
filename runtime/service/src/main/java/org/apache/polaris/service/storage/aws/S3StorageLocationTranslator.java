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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.net.URI;
import java.util.Locale;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.polaris.core.storage.RemoteSigningProperty;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.aws.S3Location;
import org.apache.polaris.service.storage.StorageLocationTranslator;

@ApplicationScoped
@Identifier("S3")
public class S3StorageLocationTranslator implements StorageLocationTranslator {

  public record S3Context(boolean pathStyleAccess) implements Context {

    public static Context fromRemoteSignRequest(RemoteSignRequest signRequest) {
      boolean pathStyleAccess =
          Boolean.parseBoolean(
              signRequest
                  .properties()
                  .getOrDefault(RemoteSigningProperty.PATH_STYLE_ACCESS.shortName(), "false"));
      return new S3Context(pathStyleAccess);
    }
  }

  /**
   * Translates HTTP(S) S3 URLs into s3:// URIs.
   *
   * <p>This implementation handles both virtual-hosted style and path style S3 URLs, and handles
   * both AWS S3 and S3-compatible ones.
   *
   * <p>The implementation is optimized for performance and does not use regular expressions.
   *
   * <p>The implementation is able to handle bucket names with dots in path style access. When using
   * virtual-hosted style, it is able to handle bucket names with dots only when the endpoint is an
   * official AWS S3 endpoint, or when the bucket name is followed by ".s3", e.g. {@code
   * my.bucket.s3.us-east-1.amazonaws.com} or {@code my.bucket.s3.minio.local:9000}.
   */
  @Override
  public StorageLocation translate(URI uri, Context context) {

    String scheme = uri.getScheme();
    if (!"https".equalsIgnoreCase(scheme) && !"http".equalsIgnoreCase(scheme)) {
      throw new IllegalArgumentException("Invalid S3 URL: " + uri);
    }

    String host = uri.getHost();
    if (host == null) {
      throw new IllegalArgumentException("Invalid S3 URL: " + uri);
    }
    host = host.toLowerCase(Locale.ROOT);

    String path = uri.getPath();
    path = path == null ? "" : path;

    String bucket;
    String key;

    if (((S3Context) context).pathStyleAccess()) {
      if (path.isEmpty() || path.equals("/")) {
        throw new IllegalArgumentException("Invalid S3 URL: " + uri); // No bucket in path
      }
      int slashIndex = path.indexOf('/', 1);
      if (slashIndex == -1) {
        // Path is just the bucket: /mybucket
        bucket = path.substring(1);
        key = "";
      } else {
        // Path is bucket + key: /mybucket/myimage.jpg
        bucket = path.substring(1, slashIndex);
        key = path.substring(slashIndex);
      }
    } else {
      // All official S3 endpoints have ".s3" as the bucket separator
      int bucketEndIndex = host.lastIndexOf(".s3");
      if (bucketEndIndex <= 0) {
        // Assume S3-like host, bucket is the first subdomain
        bucketEndIndex = host.indexOf('.');
      }
      if (bucketEndIndex <= 0) {
        throw new IllegalArgumentException("Invalid S3 URL: " + uri);
      }
      bucket = host.substring(0, bucketEndIndex);
      key = path;
    }

    if (bucket.isEmpty()) {
      throw new IllegalArgumentException("Invalid S3 URL: " + uri);
    }

    return new S3Location("s3://" + bucket + key);
  }
}
