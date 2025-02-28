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
public record ETag(boolean isWeak, @Nonnull String value) {

    protected static Pattern ETAG_PATTERN = Pattern.compile("(W/)?\"([^\"]*)\"");

    /**
     * Consumes the raw value of an entity-tag and produces a logical representation
     * @param rawValue
     */
    public static ETag fromHeader(@Nonnull String rawValue) {
        rawValue = rawValue.trim();
        Matcher matcher = ETag.ETAG_PATTERN.matcher(rawValue);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid ETag representation.");
        }

        boolean weak = rawValue.startsWith("W/");
        String value = rawValue.substring(rawValue.indexOf('"') + 1, rawValue.length() - 1);

        return new ETag(weak, value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isWeak, value);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ETag other) {
            return isWeak == other.isWeak && value.equals(other.value);
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
        String representation = isWeak ? "W/" : "";
        return representation + "\"" + value + "\"";
    }

}
