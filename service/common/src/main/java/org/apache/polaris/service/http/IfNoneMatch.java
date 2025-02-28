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

package org.apache.polaris.service.http;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * Logical representation of an HTTP compliant If-None-Match header.
 */
public class IfNoneMatch {

    private final boolean wildcard;

    private final List<ETag> etags;

    /**
     * Parses a non wildcard If-None-Match header value into ETags
     * @param header the header to parse, eg `W\"etag1", "etag2,with,comma", W\"etag3"`
     * @return the header parsed into raw string etag values. For the example given, ["W\"etag1"", ""etag2,with,comma"", "W\"etag3""]
     */
    private static List<String> parseHeaderIntoParts(String header) {
        header = header.trim();
        Matcher matcher = ETag.ETAG_PATTERN.matcher(header);

        return matcher.results()
                .map(result -> result.group(0))
                .collect(Collectors.toList());
    }

    public IfNoneMatch(boolean wildcard) {
        this.wildcard = wildcard;
        this.etags = new ArrayList<>();
    }

    public IfNoneMatch(List<ETag> etags) {
        this.wildcard = false;
        this.etags = new ArrayList<>(etags);
    }

    public static IfNoneMatch fromHeader(String rawValue) {
        if (rawValue == null) {
            return new IfNoneMatch(false);
        }

        rawValue = rawValue.trim();
        if (rawValue.equals("*")) {
            return new IfNoneMatch(true);
        } else {
            List<String> parts = parseHeaderIntoParts(rawValue);
            List<ETag> etags = parts.stream().map(ETag::fromHeader).toList();

            if (etags.isEmpty() && !rawValue.isEmpty()) {
                throw new IllegalArgumentException("Invalid representation for If-None-Match header.");
            }
            return new IfNoneMatch(etags);
        }
    }

    public boolean isWildcard() {
        return wildcard;
    }

    public List<ETag> getEtags() {
        return ImmutableList.copyOf(etags);
    }

    /**
     * If any contained ETag matches the provided etag or the header is a wildcard.
     * Only matches weak etags to weak etags and strong etags to strong etags.
     * @param etag the etag to compare against.
     * @return true if any of the contained ETags match the provided etag
     */
    public boolean anyMatch(ETag etag) {
        if (wildcard) return true;
        return etags.contains(etag);
    }

}
