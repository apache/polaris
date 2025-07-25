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
package org.apache.polaris.core.storage.oss;

import jakarta.annotation.Nonnull;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.polaris.core.storage.StorageLocation;

public class OssLocation extends StorageLocation {
    private static final Pattern URI_PATTERN = Pattern.compile("^(oss):(.+)$", Pattern.CASE_INSENSITIVE);
    private final String scheme;
    private final String locationWithoutScheme;

    public OssLocation(@Nonnull String location) {
        super(location);
        Matcher matcher = URI_PATTERN.matcher(location);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid OSS location uri " + location);
        }
        this.scheme = matcher.group(1);
        this.locationWithoutScheme = matcher.group(2);
    }

    public static boolean isOssLocation(String location) {
        if (location == null) {
            return false;
        }
        Matcher matcher = URI_PATTERN.matcher(location);
        return matcher.matches();
    }

    @Override
    public boolean isChildOf(StorageLocation potentialParent) {
        if (potentialParent instanceof OssLocation) {
            OssLocation that = (OssLocation) potentialParent;
            // Given that OSS paths need to be treated with bucket and path hierarchy
            String slashTerminatedObjectKey = ensureTrailingSlash(this.locationWithoutScheme);
            String slashTerminatedObjectKeyThat = ensureTrailingSlash(that.locationWithoutScheme);
            return slashTerminatedObjectKey.startsWith(slashTerminatedObjectKeyThat);
        }
        return false;
    }

    public String getBucket() {
        String path = locationWithoutScheme.startsWith("//") ?
                locationWithoutScheme.substring(2) : locationWithoutScheme;
        int slashIndex = path.indexOf('/');
        return slashIndex > 0 ? path.substring(0, slashIndex) : path;
    }

    public String getKey() {
        String path = locationWithoutScheme.startsWith("//") ?
                locationWithoutScheme.substring(2) : locationWithoutScheme;
        int slashIndex = path.indexOf('/');
        return slashIndex > 0 ? path.substring(slashIndex + 1) : "";
    }

    public String getScheme() {
        return scheme;
    }

    @Override
    public String withoutScheme() {
        return locationWithoutScheme;
    }
}