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
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Logical representation of an HTTP compliant If-None-Match header. */
public record IfNoneMatch(boolean isWildcard, @Nonnull List<String> eTags) {

  private static final String WILDCARD_HEADER_VALUE = "*";

  // Matches but does not capture any content of an ETag
  private static final String ETAG_REGEX = "(?:W/)?\"(?:[^\"]*)\"";

  // Builds comma separated list of ETags and disallows trailing comma
  private static final String IF_NONE_MATCH_REGEX =
      String.format("(?:%s, )*%s", ETAG_REGEX, ETAG_REGEX);

  // Wraps pattern in capture group to capture entire ETag
  private static final Pattern ETAG_PATTERN = Pattern.compile(String.format("(%s)", ETAG_REGEX));

  // Used to validate entire header pattern
  private static final Pattern IF_NONE_MATCH_PATTERN = Pattern.compile(IF_NONE_MATCH_REGEX);

  public static final IfNoneMatch EMPTY = new IfNoneMatch(List.of());

  public static final IfNoneMatch WILDCARD = new IfNoneMatch(true, List.of());

  public IfNoneMatch(List<String> etags) {
    this(false, etags);
  }

  public IfNoneMatch {
    if (isWildcard && !eTags.isEmpty()) {
      // if the header is a wildcard, it must not be constructed with
      // any etags, if it is not a wildcard, it can still contain no ETags, so
      // the converse is not true
      throw new IllegalArgumentException(
          "Invalid representation for If-None-Match header. If-None-Match cannot contain ETags if it takes "
              + "the wildcard value '*'");
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
   *
   * @param rawValue The raw value of the If-None-Match header
   * @return A logically equivalent representation of the raw header content
   */
  public static IfNoneMatch fromHeader(String rawValue) {
    // parse null header as an empty header
    if (rawValue == null) {
      return EMPTY;
    }

    rawValue = rawValue.trim();
    if (rawValue.equals(WILDCARD_HEADER_VALUE)) {
      return WILDCARD;
    } else {

      // ensure entire header matches expected pattern
      if (!IF_NONE_MATCH_PATTERN.matcher(rawValue).matches()) {
        throw new IllegalArgumentException("Invalid If-None-Match header: " + rawValue);
      }

      // extract out ETags using the capture group
      List<String> etags =
          ETAG_PATTERN.matcher(rawValue).results().map(MatchResult::group).toList();

      return new IfNoneMatch(etags);
    }
  }

  /**
   * If any contained ETag matches the provided etag or the header is a wildcard. Only matches weak
   * eTags to weak eTags and strong eTags to strong eTags.
   *
   * @param eTag the etag to compare against.
   * @return true if any of the contained ETags match the provided etag
   */
  public boolean anyMatch(String eTag) {
    if (this.isWildcard) return true;
    return eTags.contains(eTag);
  }
}
