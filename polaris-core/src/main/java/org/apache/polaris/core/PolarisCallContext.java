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
package org.apache.polaris.core;

import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.config.RealmConfigurationSource;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.jspecify.annotations.NonNull;

/**
 * The Call context is allocated each time a new REST request is processed. It contains instances of
 * low-level services required to process that request.
 *
 * <p>{@link BasePersistence} continues to carry the bulk of the metastore SPI surface (and still
 * extends {@code PolicyMappingPersistence} / acts as the {@code IntegrationPersistence} via a
 * runtime cast for now). {@link MetricsPersistence} is intentionally decoupled and supplied
 * separately so callers that only need metrics persistence do not have to depend on {@link
 * BasePersistence}.
 */
public class PolarisCallContext implements CallContext {

  // meta store which is used to persist Polaris entity metadata
  private final BasePersistence metaStore;
  private final MetricsPersistence metricsPersistence;
  private final RealmConfigurationSource configurationSource;
  private final RealmContext realmContext;
  private final RealmConfig realmConfig;

  /**
   * @deprecated Use {@link PolarisCallContext#PolarisCallContext(RealmContext, BasePersistence,
   *     MetricsPersistence, RealmConfigurationSource)}.
   */
  @SuppressWarnings("removal")
  @Deprecated(forRemoval = true)
  public PolarisCallContext(
      @NonNull RealmContext realmContext,
      @NonNull BasePersistence metaStore,
      @NonNull PolarisConfigurationStore configurationStore) {
    this(
        realmContext, metaStore, new MetricsPersistence() {}, configurationStore::getConfiguration);
  }

  /**
   * Convenience constructor for backends whose {@link BasePersistence} implementation also
   * implements {@link MetricsPersistence} (the common in-tree case). Callers that need to wire a
   * distinct metrics implementation should use {@link #PolarisCallContext(RealmContext,
   * BasePersistence, MetricsPersistence, RealmConfigurationSource)}.
   */
  public <P extends BasePersistence & MetricsPersistence> PolarisCallContext(
      @NonNull RealmContext realmContext,
      @NonNull P metaStore,
      @NonNull RealmConfigurationSource configurationSource) {
    this(realmContext, metaStore, metaStore, configurationSource);
  }

  /** Primary constructor — {@link MetricsPersistence} is supplied separately from the metastore. */
  public PolarisCallContext(
      @NonNull RealmContext realmContext,
      @NonNull BasePersistence metaStore,
      @NonNull MetricsPersistence metricsPersistence,
      @NonNull RealmConfigurationSource configurationSource) {
    this.realmContext = realmContext;
    this.metaStore = metaStore;
    this.metricsPersistence = metricsPersistence;
    this.configurationSource = configurationSource;
    this.realmConfig = new RealmConfigImpl(this.configurationSource, this.realmContext);
  }

  /** Convenience constructor that defaults to {@link RealmConfigurationSource#EMPTY_CONFIG}. */
  public PolarisCallContext(
      @NonNull RealmContext realmContext,
      @NonNull BasePersistence metaStore,
      @NonNull MetricsPersistence metricsPersistence) {
    this(realmContext, metaStore, metricsPersistence, RealmConfigurationSource.EMPTY_CONFIG);
  }

  /**
   * Convenience constructor for callers whose persistence type satisfies both SPIs and who do not
   * have a {@link RealmConfigurationSource}.
   */
  public <P extends BasePersistence & MetricsPersistence> PolarisCallContext(
      @NonNull RealmContext realmContext, @NonNull P metaStore) {
    this(realmContext, metaStore, metaStore, RealmConfigurationSource.EMPTY_CONFIG);
  }

  public BasePersistence getMetaStore() {
    return metaStore;
  }

  public MetricsPersistence getMetricsPersistence() {
    return metricsPersistence;
  }

  @Override
  public RealmContext getRealmContext() {
    return realmContext;
  }

  @Override
  public RealmConfig getRealmConfig() {
    return realmConfig;
  }

  @Override
  public PolarisCallContext getPolarisCallContext() {
    return this;
  }

  @Override
  public PolarisCallContext copy() {
    // The realm context is a request scoped bean injected by CDI,
    // which will be closed after the http request. This copy is currently
    // only used by TaskExecutor right before the task is handled, since the
    // task is executed outside the active request scope, we need to make a
    // copy of the RealmContext to ensure the access during the task executor.
    String realmId = this.realmContext.getRealmIdentifier();
    RealmContext realmContext = () -> realmId;
    return new PolarisCallContext(
        realmContext, this.metaStore, this.metricsPersistence, this.configurationSource);
  }
}
