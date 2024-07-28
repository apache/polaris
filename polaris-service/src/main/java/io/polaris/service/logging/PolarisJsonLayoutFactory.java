/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.polaris.service.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.pattern.ExtendedThrowableProxyConverter;
import ch.qos.logback.classic.pattern.RootCauseFirstThrowableProxyConverter;
import ch.qos.logback.classic.pattern.ThrowableHandlingConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.LayoutBase;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.logging.json.AbstractJsonLayoutBaseFactory;
import io.dropwizard.logging.json.EventAttribute;
import io.dropwizard.logging.json.layout.EventJsonLayout;
import io.dropwizard.logging.json.layout.ExceptionFormat;
import io.dropwizard.logging.json.layout.JsonFormatter;
import io.dropwizard.logging.json.layout.TimestampFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Basically a direct copy of {@link io.dropwizard.logging.json.EventJsonLayoutBaseFactory} that
 * adds support for {@link ILoggingEvent#getKeyValuePairs()} in the output. By default, additional
 * key/value pairs are included as the `params` field of the json output, but they can optionally be
 * flattened into the log event output.
 *
 * <p>To use this appender, change the appender type to `polaris` <code>
 * loggers:
 *   org.apache.iceberg.rest: DEBUG
 *   org.apache.iceberg.polaris: DEBUG
 *   appenders:
 *     - type: console
 *       threshold: ALL
 *       layout:
 *         type: polaris
 *         flattenKeyValues: false
 *         includeKeyValues: true
 * </code>
 */
@JsonTypeName("polaris")
public class PolarisJsonLayoutFactory extends AbstractJsonLayoutBaseFactory<ILoggingEvent> {
  private EnumSet<EventAttribute> includes =
      EnumSet.of(
          EventAttribute.LEVEL,
          EventAttribute.THREAD_NAME,
          EventAttribute.MDC,
          EventAttribute.MARKER,
          EventAttribute.LOGGER_NAME,
          EventAttribute.MESSAGE,
          EventAttribute.EXCEPTION,
          EventAttribute.TIMESTAMP);

  private Set<String> includesMdcKeys = Collections.emptySet();
  private boolean flattenMdc = false;
  private boolean includeKeyValues = true;
  private boolean flattenKeyValues = false;

  @Nullable private ExceptionFormat exceptionFormat;

  @JsonProperty
  public EnumSet<EventAttribute> getIncludes() {
    return includes;
  }

  @JsonProperty
  public void setIncludes(EnumSet<EventAttribute> includes) {
    this.includes = includes;
  }

  @JsonProperty
  public Set<String> getIncludesMdcKeys() {
    return includesMdcKeys;
  }

  @JsonProperty
  public void setIncludesMdcKeys(Set<String> includesMdcKeys) {
    this.includesMdcKeys = includesMdcKeys;
  }

  @JsonProperty
  public boolean isFlattenMdc() {
    return flattenMdc;
  }

  @JsonProperty
  public void setFlattenMdc(boolean flattenMdc) {
    this.flattenMdc = flattenMdc;
  }

  @JsonProperty
  public boolean isIncludeKeyValues() {
    return includeKeyValues;
  }

  @JsonProperty
  public void setIncludeKeyValues(boolean includeKeyValues) {
    this.includeKeyValues = includeKeyValues;
  }

  @JsonProperty
  public boolean isFlattenKeyValues() {
    return flattenKeyValues;
  }

  @JsonProperty
  public void setFlattenKeyValues(boolean flattenKeyValues) {
    this.flattenKeyValues = flattenKeyValues;
  }

  /**
   * @since 2.0
   */
  @JsonProperty("exception")
  public void setExceptionFormat(ExceptionFormat exceptionFormat) {
    this.exceptionFormat = exceptionFormat;
  }

  /**
   * @since 2.0
   */
  @JsonProperty("exception")
  @Nullable
  public ExceptionFormat getExceptionFormat() {
    return exceptionFormat;
  }

  @Override
  public LayoutBase<ILoggingEvent> build(LoggerContext context, TimeZone timeZone) {
    final PolarisJsonLayout jsonLayout =
        new PolarisJsonLayout(
            createDropwizardJsonFormatter(),
            createTimestampFormatter(timeZone),
            createThrowableProxyConverter(context),
            includes,
            getCustomFieldNames(),
            getAdditionalFields(),
            includesMdcKeys,
            flattenMdc,
            includeKeyValues,
            flattenKeyValues);
    jsonLayout.setContext(context);
    return jsonLayout;
  }

  public static class PolarisJsonLayout extends EventJsonLayout {
    private final boolean includeKeyValues;
    private final boolean flattenKeyValues;

    public PolarisJsonLayout(
        JsonFormatter jsonFormatter,
        TimestampFormatter timestampFormatter,
        ThrowableHandlingConverter throwableProxyConverter,
        Set<EventAttribute> includes,
        Map<String, String> customFieldNames,
        Map<String, Object> additionalFields,
        Set<String> includesMdcKeys,
        boolean flattenMdc,
        boolean includeKeyValues,
        boolean flattenKeyValues) {
      super(
          jsonFormatter,
          timestampFormatter,
          throwableProxyConverter,
          includes,
          customFieldNames,
          additionalFields,
          includesMdcKeys,
          flattenMdc);
      this.includeKeyValues = includeKeyValues;
      this.flattenKeyValues = flattenKeyValues;
    }

    @Override
    protected Map<String, Object> toJsonMap(ILoggingEvent event) {
      Map<String, Object> jsonMap = super.toJsonMap(event);
      if (!includeKeyValues) {
        return jsonMap;
      }
      Map<String, Object> keyValueMap =
          event.getKeyValuePairs() == null
              ? Map.of()
              : event.getKeyValuePairs().stream()
                  .collect(Collectors.toMap(kv -> kv.key, kv -> kv.value));
      ImmutableMap.Builder<String, Object> builder =
          ImmutableMap.<String, Object>builder().putAll(jsonMap);
      if (flattenKeyValues) {
        builder.putAll(keyValueMap);
      } else {
        builder.put("params", keyValueMap);
      }
      return builder.build();
    }
  }

  protected ThrowableHandlingConverter createThrowableProxyConverter(LoggerContext context) {
    if (exceptionFormat == null) {
      return new RootCauseFirstThrowableProxyConverter();
    }

    ThrowableHandlingConverter throwableHandlingConverter;
    if (exceptionFormat.isRootFirst()) {
      throwableHandlingConverter = new RootCauseFirstThrowableProxyConverter();
    } else {
      throwableHandlingConverter = new ExtendedThrowableProxyConverter();
    }

    List<String> options = new ArrayList<>();
    // depth must be added first
    options.add(exceptionFormat.getDepth());
    options.addAll(exceptionFormat.getEvaluators());

    throwableHandlingConverter.setOptionList(options);
    throwableHandlingConverter.setContext(context);

    return throwableHandlingConverter;
  }
}
