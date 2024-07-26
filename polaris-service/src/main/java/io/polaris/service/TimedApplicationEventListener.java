package io.polaris.service;

import com.google.common.base.Stopwatch;
import io.micrometer.core.instrument.MeterRegistry;
import io.polaris.core.context.CallContext;
import io.polaris.core.monitor.PolarisMetricRegistry;
import io.polaris.service.resource.TimedApi;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.ext.Provider;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

/**
 * An ApplicationEventListener that supports timing of resource method execution and error counting.
 * It uses Micrometer for metrics collection and provides detailed metrics tagged with realm
 * identifiers and distinguishes between successful executions and errors.
 */
@Provider
public class TimedApplicationEventListener implements ApplicationEventListener {

  // The PolarisMetricRegistry instance used for recording metrics and error counters.
  private final PolarisMetricRegistry polarisMetricRegistry;

  public TimedApplicationEventListener(MeterRegistry meterRegistry) {
    this.polarisMetricRegistry = new PolarisMetricRegistry(meterRegistry);
  }

  @Override
  public void onEvent(ApplicationEvent event) {}

  @Override
  public RequestEventListener onRequest(RequestEvent event) {
    return new TimedRequestEventListener();
  }

  /**
   * A RequestEventListener implementation that handles timing of resource method execution and
   * increments error counters on failures. The lifetime of the listener is tied to a single HTTP
   * request.
   */
  private class TimedRequestEventListener implements RequestEventListener {
    private String metric;
    private Stopwatch sw;

    /** Handles various types of RequestEvents to start timing, stop timing, and record metrics. */
    @Override
    public void onEvent(RequestEvent event) {
      if (event.getType() == RequestEvent.Type.RESOURCE_METHOD_START) {
        Method method =
            event.getUriInfo().getMatchedResourceMethod().getInvocable().getHandlingMethod();
        if (method.isAnnotationPresent(TimedApi.class)) {
          TimedApi timedApi = method.getAnnotation(TimedApi.class);
          metric = timedApi.value();
          sw = Stopwatch.createStarted();
        }

      } else if (event.getType() == RequestEvent.Type.FINISHED && metric != null) {
        String realmId = CallContext.getCurrentContext().getRealmContext().getRealmIdentifier();
        if (event.isSuccess()) {
          sw.stop();
          polarisMetricRegistry.recordTimer(metric, sw.elapsed(TimeUnit.MILLISECONDS), realmId);
        } else {
          int statusCode = event.getContainerResponse().getStatus();
          polarisMetricRegistry.incrementErrorCounter(metric, statusCode, realmId);
        }
      }
    }
  }
}
