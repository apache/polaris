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

package org.apache.polaris.core.persistence.pagination;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.ServiceLoader;

final class PageTokenUtil {

  private static final ObjectMapper SMILE_MAPPER = new SmileMapper().findAndRegisterModules();

  /** Constant for {@link PageToken#readEverything()}. */
  static final PageToken READ_EVERYTHING =
      new PageToken() {
        @Override
        public OptionalInt pageSize() {
          return OptionalInt.empty();
        }

        @Override
        public Optional<Token> value() {
          return Optional.empty();
        }

        @Override
        public int hashCode() {
          return 1;
        }

        @Override
        public boolean equals(Object obj) {
          if (!(obj instanceof PageToken)) {
            return false;
          }
          PageToken other = (PageToken) obj;
          return other.pageSize().isEmpty() && other.value().isEmpty();
        }

        @Override
        public String toString() {
          return "PageToken(everything)";
        }
      };

  static PageToken fromLimit(int limit) {
    return new PageToken() {
      @Override
      public OptionalInt pageSize() {
        return OptionalInt.of(limit);
      }

      @Override
      public Optional<Token> value() {
        return Optional.empty();
      }

      @Override
      public int hashCode() {
        return 2;
      }

      @Override
      public boolean equals(Object obj) {
        if (!(obj instanceof PageToken)) {
          return false;
        }
        PageToken other = (PageToken) obj;
        return other.pageSize().equals(pageSize()) && other.value().isEmpty();
      }

      @Override
      public String toString() {
        return "PageToken(limit = " + limit + ")";
      }
    };
  }

  private PageTokenUtil() {}

  /**
   * Decodes a {@link PageToken} from API request parameters for the page-size and a serialized page
   * token.
   */
  static PageToken decodePageRequest(
      @Nullable String requestedPageToken, @Nullable Integer requestedPageSize) {
    if (requestedPageToken != null) {
      var bytes = Base64.getUrlDecoder().decode(requestedPageToken);
      try {
        var pageToken = SMILE_MAPPER.readValue(bytes, PageToken.class);
        if (requestedPageSize != null) {
          int pageSizeInt = requestedPageSize;
          checkArgument(pageSizeInt >= 0, "Invalid page size");
          if (pageToken.pageSize().orElse(-1) != pageSizeInt) {
            pageToken = ImmutablePageToken.builder().from(pageToken).pageSize(pageSizeInt).build();
          }
        }
        return pageToken;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else if (requestedPageSize != null) {
      int pageSizeInt = requestedPageSize;
      checkArgument(pageSizeInt >= 0, "Invalid page size");
      return fromLimit(pageSizeInt);
    } else {
      return READ_EVERYTHING;
    }
  }

  /**
   * Returns the encoded ({@link String} serialized) {@link PageToken} built from the given {@link
   * PageToken currentPageToken}, the page token of the current request, and {@link Token
   * nextToken}, the token for the next page.
   *
   * @param currentPageToken page token of the currently handled API request, must not be {@code
   *     null}
   * @param nextToken token for the next page, can be {@code null}, in which case the result will be
   *     {@code null}
   * @return base-64/url-encoded serialized {@link PageToken} for the next page.
   */
  static @Nullable String encodePageToken(PageToken currentPageToken, @Nullable Token nextToken) {
    if (nextToken == null) {
      return null;
    }

    return serializePageToken(
        ImmutablePageToken.builder()
            .pageSize(currentPageToken.pageSize())
            .value(nextToken)
            .build());
  }

  /**
   * Serializes the given {@link PageToken pageToken}
   *
   * @return base-64/url-encoded serialized {@link PageToken} for the next page.
   */
  @VisibleForTesting
  static @Nullable String serializePageToken(PageToken pageToken) {
    if (pageToken == null) {
      return null;
    }

    try {
      var serialized = SMILE_MAPPER.writeValueAsBytes(pageToken);
      return Base64.getUrlEncoder().encodeToString(serialized);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /** Lazily initialized registry of all token-types. */
  private static final class Registry {
    private static final Map<String, Token.TokenType> BY_ID;

    static {
      var byId = new HashMap<String, Token.TokenType>();
      var loader = ServiceLoader.load(Token.TokenType.class);
      loader.stream()
          .map(ServiceLoader.Provider::get)
          .forEach(
              tokenType -> {
                var ex = byId.put(tokenType.id(), tokenType);
                if (ex != null) {
                  throw new IllegalStateException(
                      format("Duplicate token type ID: from %s and %s", tokenType, ex));
                }
              });
      BY_ID = unmodifiableMap(byId);
    }
  }

  /**
   * Jackson type-id resolver, resolves a {@link Token#getT() token type value} to a concrete Java
   * type, consulting the {@link Registry}.
   */
  static final class TokenTypeIdResolver extends TypeIdResolverBase {
    private JavaType baseType;

    public TokenTypeIdResolver() {}

    @Override
    public void init(JavaType bt) {
      baseType = bt;
    }

    @Override
    public String idFromValue(Object value) {
      return getId(value);
    }

    @Override
    public String idFromValueAndType(Object value, Class<?> suggestedType) {
      return getId(value);
    }

    @Override
    public JsonTypeInfo.Id getMechanism() {
      return JsonTypeInfo.Id.CUSTOM;
    }

    private String getId(Object value) {
      if (value instanceof Token) {
        return ((Token) value).getT();
      }

      return null;
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) {
      var idLower = id.toLowerCase(Locale.ROOT);
      var asType = Registry.BY_ID.get(idLower);
      if (asType == null) {
        throw new IllegalStateException("Cannot deserialize paging token value of type " + idLower);
      }
      if (baseType.getRawClass().isAssignableFrom(asType.javaType())) {
        return context.constructSpecializedType(baseType, asType.javaType());
      }

      // This is rather a "test-only" code path, but it might happen in real life as well, when
      // calling the ObjectMapper with a "too specific" type and not just Change.class.
      // So we can get here for example, if the baseType (induced by the type passed to
      // ObjectMapper), is GenericChange.class, but the type is a "well known" type like
      // ChangeRename.class.
      @SuppressWarnings("unchecked")
      var concrete = (Class<? extends Token>) baseType.getRawClass();
      return context.constructSpecializedType(baseType, concrete);
    }
  }
}
