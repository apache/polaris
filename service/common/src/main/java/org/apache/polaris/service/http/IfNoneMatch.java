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

import jakarta.annotation.Nonnull;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Logical representation of an HTTP compliant If-None-Match header.
 */
public record IfNoneMatch(boolean isWildcard, @Nonnull List<String> eTags) {

    protected static Pattern ETAG_PATTERN = Pattern.compile("(W/)?\"([^\"]*)\"");

    public IfNoneMatch(List<String> etags) {
        this(false, etags);
    }

    public IfNoneMatch {
        if (isWildcard && !eTags.isEmpty()) {
            // if the header is a wildcard, it must not be constructed with
            // any etags, if it is not a wildcard, it can still contain no ETags, so
            // the converse is not true
            throw new IllegalArgumentException(
                    "Invalid representation for If-None-Match header. If-None-Match cannot contain ETags if it takes " +
                            "the wildcard value '*'");
        }

        for (String etag : eTags) {
            Matcher matcher = ETAG_PATTERN.matcher(etag);

            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid ETag representation: " + etag);
            }
        }
    }

    /**
     * Parses the raw content of an If-None-Match header into the logical representation
     * @param rawValue The raw value of the If-None-Match header
     * @return A logically equivalent representation of the raw header content
     */
    public static IfNoneMatch fromHeader(String rawValue) {
        // parse null header as an empty header
        if (rawValue == null) {
            return new IfNoneMatch(List.of());
        }

        rawValue = rawValue.trim();
        if (rawValue.equals("*")) {
            return IfNoneMatch.wildcard();
        } else {

            List<String> parts = Stream.of(rawValue.split("\\s+")) // Tokenizes string , eg. we will now have [`W/"etag1",`, `W/"etag2"`]
                    .map(s -> s.endsWith(",") ? s.substring(0, s.length() - 1) : s) // Remove trailing comma from each part, so we now have [`W/"etag1"`, `W/"etag2"`]
                    .toList();

            // ensure all of the individual tags are valid
            boolean allValid = parts.stream().allMatch(s -> {
                Matcher matcher = ETAG_PATTERN.matcher(s);
                return matcher.matches();
            });

            if (!allValid) {
                throw new IllegalArgumentException("Invalid If-None-Match value provided: " + rawValue);
            }

            return new IfNoneMatch(parts);
        }
    }

    /**
     * Constructs a new wildcard If-None-Match header *
     * @return
     */
    public static IfNoneMatch wildcard() {
        return new IfNoneMatch(true, List.of());
    }

    /**
     * If any contained ETag matches the provided etag or the header is a wildcard.
     * Only matches weak eTags to weak eTags and strong eTags to strong eTags.
     * @param eTag the etag to compare against.
     * @return true if any of the contained ETags match the provided etag
     */
    public boolean anyMatch(String eTag) {
        if (this.isWildcard) return true;
        return eTags.contains(eTag);
    }

}
