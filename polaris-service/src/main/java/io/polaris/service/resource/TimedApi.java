package io.polaris.service.resource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark methods for timing API calls and counting errors. The backing logic is
 * controlled by {@link io.polaris.service.TimedApplicationEventListener}, therefore this annotation
 * is only effective for Jersey resource methods.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface TimedApi {
  /**
   * The name of the metric to be recorded.
   *
   * @return the metric name
   */
  String value();
}
