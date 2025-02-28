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

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * HTTP Compliant ETag logical wrapper.
 */
public class ETag {

    protected static Pattern ETAG_PATTERN = Pattern.compile("(W/)?\"([^\"]*)\"");

    private final boolean weak;

    private final String value;

    public ETag(boolean weak, String value) {
        this.weak = weak;
        this.value = value;
    }

    /**
     * Consumes the raw value of an entity-tag and produces a logical representation
     * @param rawValue
     */
    public ETag(@Nonnull String rawValue) {
        rawValue = rawValue.trim();
        Matcher matcher = ETag.ETAG_PATTERN.matcher(rawValue);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid ETag representation.");
        }

        this.weak = rawValue.startsWith("W/");
        this.value = rawValue.substring(rawValue.indexOf('"') + 1, rawValue.length() - 1);
    }

    /**
     * Obtain the value of the etag. For example, if we had an etag W/"abc", this
     * method would return abc
     * @return The internal value of the etag
     */
    public String value() {
        return value;
    }

    /**
     * Determine if the etag is a weak etag
     * @return true if the etag is prefixed by W/, false otherwise
     */
    public boolean isWeak() {
        return weak;
    }

    @Override
    public int hashCode() {
        return Objects.hash(weak, value);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ETag other) {
            return weak == other.weak && value.equals(other.value);
        } else if (o instanceof String s) {
            return this.toString().equals(s);
        }
        return false;
    }

    /**
     * Returns the raw HTTP compliant representation of this tag
     * @return
     */
    @Override
    public String toString() {
        String representation = weak ? "W/" : "";
        return representation + "\"" + value + "\"";
    }

}
