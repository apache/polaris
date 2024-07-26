package io.polaris.service.tracing;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import jakarta.servlet.http.HttpServletRequest;
import java.net.http.HttpRequest;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link TextMapSetter} and {@link TextMapGetter} that can handle an {@link
 * HttpServletRequest} for extracting headers and sets headers on a {@link HttpRequest.Builder}.
 */
public class HeadersMapAccessor
    implements TextMapSetter<HttpRequest.Builder>, TextMapGetter<HttpServletRequest> {
  @Override
  public Iterable<String> keys(HttpServletRequest carrier) {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                carrier.getHeaderNames().asIterator(), Spliterator.IMMUTABLE),
            false)
        .toList();
  }

  @Nullable
  @Override
  public String get(@Nullable HttpServletRequest carrier, String key) {
    return carrier == null ? null : carrier.getHeader(key);
  }

  @Override
  public void set(@Nullable HttpRequest.Builder carrier, String key, String value) {
    if (carrier != null) {
      carrier.setHeader(key, value);
    }
  }
}
