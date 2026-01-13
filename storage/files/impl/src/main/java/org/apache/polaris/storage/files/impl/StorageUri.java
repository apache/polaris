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
package org.apache.polaris.storage.files.impl;

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.net.URI;
import java.util.Comparator;
import java.util.Locale;
import java.util.Objects;

/**
 * A utility class for working with file storage locations to be used instead of {@link URI} since
 * many actual object store locations do not always match the URI syntax.
 *
 * <p>This type is <em>private</em> to the {@code polaris-storage-files-impl} module.
 */
final class StorageUri implements Comparable<StorageUri> {
  public static final String SCHEME_FILE = "file";

  public static final Comparator<StorageUri> COMPARATOR =
      Comparator.<StorageUri, String>comparing(u -> normalizedForCompare(u.scheme))
          .thenComparing(u -> normalizedForCompare(u.authority))
          .thenComparing(u -> normalizedForCompare(u.path));

  private final String scheme;
  private final String authority;
  private final String path;

  private StorageUri(String location) {
    // `file:` URIs may come in different forms that are considered equivalent by java.net.URI
    // Convert to the form used by File.toURI().toString()
    this.scheme = scheme(location);
    if (scheme != null) {
      var schemeSpecific = location.substring(scheme.length() + 1);
      if (schemeSpecific.startsWith("//")) {
        int pathPos = schemeSpecific.indexOf('/', 2);
        if (pathPos < 0) {
          this.authority = emptyToNull(schemeSpecific.substring(2));
          this.path = null;
        } else {
          this.authority = emptyToNull(schemeSpecific.substring(2, pathPos));
          this.path = normalizedPath(schemeSpecific.substring(pathPos));
        }
      } else {
        this.authority = null;
        this.path = normalizedPath(schemeSpecific);
      }
    } else {
      this.authority = null;
      this.path = normalizedPath(location);
    }
  }

  private StorageUri(String scheme, String authority, String path) {
    this.scheme = scheme;
    this.authority = authority;
    this.path = path;
  }

  public String location() {
    var location = new StringBuilder();
    if (scheme != null) {
      location.append(scheme);
      location.append(':');
    }

    if (authority != null) {
      location.append("//");
      location.append(authority);
    }

    if (path != null) {
      location.append(path);
    }

    return location.toString();
  }

  public static StorageUri of(String location) {
    return new StorageUri(location);
  }

  public static StorageUri of(URI uri) {
    return of(uri.toString());
  }

  @Override
  public String toString() {
    return location();
  }

  @Nullable
  private static String scheme(String location) {
    var schemePos = location.indexOf(':');
    if (schemePos > 0) {
      return location.substring(0, schemePos).toLowerCase(Locale.ROOT);
    }
    return null;
  }

  @Nullable
  public String scheme() {
    return scheme;
  }

  @Nullable
  public String authority() {
    return authority;
  }

  @Nonnull
  public String requiredAuthority() {
    return Preconditions.checkNotNull(
        authority, "Missing required authority in storage location: %s", this);
  }

  @Nullable
  public String path() {
    return path;
  }

  @Nonnull
  public String requiredPath() {
    return Preconditions.checkNotNull(path, "Missing required path in storage location: %s", this);
  }

  public String pathWithoutLeadingTrailingSlash() {
    var p = path;
    if (p == null) {
      return "";
    }
    if (p.startsWith("/")) {
      p = p.length() == 1 ? "" : p.substring(1);
    }
    if (p.endsWith("/")) {
      p = p.substring(0, p.length() - 1);
    }
    return p;
  }

  public boolean isAbsolute() {
    return scheme != null;
  }

  public StorageUri withTrailingSeparator() {
    if (path == null) {
      return new StorageUri(scheme, authority, "/");
    } else if (!path.endsWith("/")) {
      return new StorageUri(scheme, authority, path + "/");
    }

    return this;
  }

  @Override
  public int compareTo(@Nonnull StorageUri other) {
    return COMPARATOR.compare(this, other);
  }

  @Override
  public int hashCode() {
    var h = 1;
    h = 31 * h + (scheme == null ? 0 : scheme.hashCode());
    h = 31 * h + (authority == null ? 0 : authority.hashCode());
    h = 31 * h + (path == null ? 0 : path.hashCode());
    return h;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof StorageUri o)) {
      return false;
    }

    return Objects.equals(this.scheme, o.scheme)
        && Objects.equals(this.authority, o.authority)
        && Objects.equals(this.path, o.path);
  }

  private static String normalizedForCompare(String value) {
    return value == null ? "" : value;
  }

  private static String emptyToNull(String value) {
    return (value == null || value.isEmpty()) ? null : value;
  }

  private static String normalizedPath(String loc) {
    var norm = new StringBuilder(loc.length());
    for (int pos = 0; pos < loc.length(); ) {
      int idx = loc.indexOf('/', pos);
      if (idx < 0) {
        idx = loc.length();
      } else {
        idx++; // skip the '/'
      }

      norm.append(loc, pos, idx);
      // drop excessive path separators
      while (idx < loc.length() && loc.charAt(idx) == '/') {
        idx++;
      }
      pos = idx;
    }
    return norm.toString();
  }

  public StorageUri relativize(StorageUri child) {
    if (!Objects.equals(this.scheme, child.scheme)
        || !Objects.equals(this.authority, child.authority)) {
      return child;
    }

    var nested = child.path == null ? "/" : child.path;
    var base = path == null ? "/" : path;
    if (!base.endsWith("/")) {
      base += "/";
    }

    if (!nested.startsWith(base)) {
      return child;
    }

    var rel = nested.substring(base.length());
    return of(rel);
  }

  public StorageUri relativize(String subLocation) {
    return relativize(StorageUri.of(subLocation));
  }

  public StorageUri resolve(StorageUri rel) {
    if (rel.scheme() != null) {
      return rel; // `rel` is not relative
    }

    Preconditions.checkArgument(
        !normalizedForCompare(rel.path).startsWith("."),
        "Parent and self-references are not supported: %s",
        rel.path);

    if (rel.path.startsWith("/")) { // absolute path
      return new StorageUri(scheme, authority, rel.path);
    }

    if (path == null) {
      return new StorageUri(scheme, authority, "/" + rel.path);
    }

    if (!path.startsWith("/")) { // `this` is an opaque URI
      return rel;
    }

    if (path.endsWith("/")) {
      return new StorageUri(scheme, authority, path + rel.path);
    }

    var pos = path.lastIndexOf('/');
    var basePath = path.substring(0, pos + 1);
    return new StorageUri(scheme, authority, basePath + rel.path);
  }

  public StorageUri resolve(String subPath) {
    return resolve(StorageUri.of(subPath));
  }
}
