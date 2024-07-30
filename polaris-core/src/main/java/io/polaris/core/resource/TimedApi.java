package io.polaris.core.resource;

import io.polaris.core.monitor.PolarisMetricRegistry;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify metrics to be registered on initialization. Users need to explicitly call
 * {@link PolarisMetricRegistry#init} to register the metrics.
 *
 * <p>If used on a Jersey resource method, this annotation also serves as a marker for the {@link
 * io.polaris.service.TimedApplicationEventListener} to time the underlying method and count errors
 * on failures.
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
