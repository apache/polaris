package io.polaris.core.monitor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Manages metrics for Polaris applications, providing functionality to record timers and increment
 * error counters. Also records the same for a realm-specific metric by appending a suffix and
 * tagging with the realm ID. Utilizes Micrometer for metrics collection.
 */
public class PolarisMetricRegistry {
  private final MeterRegistry meterRegistry;
  private final ConcurrentMap<String, Timer> timers = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Counter> errorCounters = new ConcurrentHashMap<>();
  private static final String TAG_REALM = "REALM_ID";
  private static final String TAG_RESP_CODE = "HTTP_RESPONSE_CODE";
  private static final String SUFFIX_ERROR = ".error";
  private static final String SUFFIX_REALM = ".realm";

  public PolarisMetricRegistry(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  public void recordTimer(String metric, long elapsedTimeMs, String realmId) {
    Timer timer =
        timers.computeIfAbsent(metric, m -> Timer.builder(metric).register(meterRegistry));
    timer.record(elapsedTimeMs, TimeUnit.MILLISECONDS);

    Timer timerRealm =
        timers.computeIfAbsent(
            metric + SUFFIX_REALM,
            m ->
                Timer.builder(metric + SUFFIX_REALM)
                    .tag(TAG_REALM, realmId)
                    .register(meterRegistry));
    timerRealm.record(elapsedTimeMs, TimeUnit.MILLISECONDS);
  }

  public void incrementErrorCounter(String metric, int statusCode, String realmId) {
    String errorMetric = metric + SUFFIX_ERROR;
    Counter errorCounter =
        errorCounters.computeIfAbsent(
            errorMetric,
            m ->
                Counter.builder(errorMetric)
                    .tag(TAG_RESP_CODE, String.valueOf(statusCode))
                    .register(meterRegistry));
    errorCounter.increment();

    Counter errorCounterRealm =
        errorCounters.computeIfAbsent(
            errorMetric + SUFFIX_REALM,
            m ->
                Counter.builder(errorMetric + SUFFIX_REALM)
                    .tag(TAG_RESP_CODE, String.valueOf(statusCode))
                    .tag(TAG_REALM, realmId)
                    .register(meterRegistry));
    errorCounterRealm.increment();
  }
}
