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
package org.apache.polaris.service;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.service.config.PolarisApplicationConfig.REQUEST_BODY_BYTES_NO_LIMIT;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleValueInstantiators;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.oauth.OAuthCredentialAuthFilter;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.semconv.ServiceAttributes;
import io.prometheus.metrics.exporter.servlet.jakarta.PrometheusMetricsServlet;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterRegistration;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Feature;
import jakarta.ws.rs.core.FeatureContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisGrantManager;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.context.RealmScope;
import org.apache.polaris.core.monitor.PolarisMetricRegistry;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.cache.PolarisRemoteCache;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.service.admin.PolarisServiceImpl;
import org.apache.polaris.service.admin.api.PolarisCatalogsApi;
import org.apache.polaris.service.admin.api.PolarisCatalogsApiService;
import org.apache.polaris.service.admin.api.PolarisPrincipalRolesApi;
import org.apache.polaris.service.admin.api.PolarisPrincipalRolesApiService;
import org.apache.polaris.service.admin.api.PolarisPrincipalsApi;
import org.apache.polaris.service.admin.api.PolarisPrincipalsApiService;
import org.apache.polaris.service.auth.TokenBrokerFactory;
import org.apache.polaris.service.catalog.IcebergCatalogAdapter;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApi;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApiService;
import org.apache.polaris.service.catalog.api.IcebergRestConfigurationApi;
import org.apache.polaris.service.catalog.api.IcebergRestConfigurationApiService;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2Api;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2ApiService;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.config.Serializers;
import org.apache.polaris.service.config.TaskHandlerConfiguration;
import org.apache.polaris.service.context.CallContextCatalogFactory;
import org.apache.polaris.service.context.CallContextResolver;
import org.apache.polaris.service.context.PolarisCallContextCatalogFactory;
import org.apache.polaris.service.context.RealmContextResolver;
import org.apache.polaris.service.context.RealmScopeContext;
import org.apache.polaris.service.exception.IcebergExceptionMapper;
import org.apache.polaris.service.exception.IcebergJerseyViolationExceptionMapper;
import org.apache.polaris.service.exception.IcebergJsonProcessingExceptionMapper;
import org.apache.polaris.service.exception.PolarisExceptionMapper;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.apache.polaris.service.persistence.cache.EntityCacheFactory;
import org.apache.polaris.service.ratelimiter.RateLimiter;
import org.apache.polaris.service.ratelimiter.RateLimiterFilter;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.ManifestFileCleanupTaskHandler;
import org.apache.polaris.service.task.TableCleanupTaskHandler;
import org.apache.polaris.service.task.TaskExecutor;
import org.apache.polaris.service.task.TaskExecutorImpl;
import org.apache.polaris.service.task.TaskFileIOSupplier;
import org.apache.polaris.service.throttling.StreamReadConstraintsExceptionMapper;
import org.apache.polaris.service.tracing.TracingFilter;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.hk2.api.Context;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.hk2.api.TypeLiteral;
import org.glassfish.hk2.utilities.ServiceLocatorUtilities;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

public class PolarisApplication extends Application<PolarisApplicationConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisApplication.class);
  private ServiceLocator serviceLocator;

  public static void main(final String[] args) throws Exception {
    new PolarisApplication().run(args);
    printAsciiArt();
  }

  private static void printAsciiArt() throws IOException {
    URL url = PolarisApplication.class.getResource("banner.txt");
    try (InputStream in =
        requireNonNull(url, "banner.txt not found on classpath")
            .openConnection()
            .getInputStream()) {
      System.out.println(new String(in.readAllBytes(), UTF_8));
    }
  }

  @Override
  public void initialize(Bootstrap<PolarisApplicationConfig> bootstrap) {
    // Enable variable substitution with environment variables
    EnvironmentVariableSubstitutor substitutor = new EnvironmentVariableSubstitutor(false);
    SubstitutingSourceProvider provider =
        new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(), substitutor);
    bootstrap.setConfigurationSourceProvider(provider);

    bootstrap.addCommand(new BootstrapRealmsCommand());
    bootstrap.addCommand(new PurgeRealmsCommand());
    SimpleModule module = new SimpleModule();
    serviceLocator = ServiceLocatorUtilities.createAndPopulateServiceLocator();
    SimpleValueInstantiators instantiators = new SimpleValueInstantiators();
    ObjectMapper objectMapper = bootstrap.getObjectMapper();
    DeserializationConfig deserializationConfig = objectMapper.getDeserializationConfig();
    serviceLocator
        .getDescriptors((c) -> true)
        .forEach(
            descriptor -> {
              try {
                Class<?> klazz =
                    PolarisApplication.class
                        .getClassLoader()
                        .loadClass(descriptor.getImplementation());
                String name = descriptor.getName();
                if (name == null) {
                  objectMapper.registerSubtypes(klazz);
                } else {
                  objectMapper.registerSubtypes(new NamedType(klazz, name));
                }
              } catch (ClassNotFoundException e) {
                LOGGER.error("Error loading class {}", descriptor.getImplementation(), e);
                throw new RuntimeException("Unable to start Polaris application");
              }
            });
    module.setValueInstantiators(instantiators);
    module.setMixInAnnotation(Authenticator.class, NamedAuthenticator.class);
    objectMapper.registerModule(module);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
  static final class NamedAuthenticator {}

  @Override
  public void run(PolarisApplicationConfig configuration, Environment environment) {
    OpenTelemetry openTelemetry = setupTracing();
    environment
        .jersey()
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(RealmScopeContext.class)
                    .in(Singleton.class)
                    .to(new TypeLiteral<Context<RealmScope>>() {});
                bind(configuration.getMetaStoreManagerFactory()).to(MetaStoreManagerFactory.class);
                bindFactory(PolarisMetaStoreManagerFactory.class)
                    .to(PolarisMetaStoreManager.class)
                    .in(RealmScope.class);

                bind(configuration.getConfigurationStore()).to(PolarisConfigurationStore.class);
                bind(configuration.getFileIOFactory()).to(FileIOFactory.class);
                bind(configuration.getPolarisAuthenticator()).to(Authenticator.class);
                bind(configuration.getTokenBrokerFactory()).to(TokenBrokerFactory.class);
                bind(configuration.getOauth2Service()).to(IcebergRestOAuth2ApiService.class);
                bind(configuration.getCallContextResolver()).to(CallContextResolver.class);
                bind(configuration.getRealmContextResolver()).to(RealmContextResolver.class);
                bind(configuration.getRateLimiter()).to(RateLimiter.class);
                bindFactory(EntityCacheFactory.class).in(RealmScope.class).to(EntityCache.class);

                bindFactory(PolarisRemoteCacheFactory.class)
                    .in(RealmScope.class)
                    .to(PolarisRemoteCache.class);

                // factory to use a cache delegating grant cache
                // currently depends explicitly on the metaStoreManager as the delegate grant
                // manager
                bindFactory(PolarisMetaStoreManagerFactory.class)
                    .in(RealmScope.class)
                    .to(PolarisGrantManager.class);
                bind(new PolarisStorageIntegrationProviderImpl(
                        () -> {
                          StsClientBuilder stsClientBuilder = StsClient.builder();
                          AwsCredentialsProvider awsCredentialsProvider =
                              configuration.credentialsProvider();
                          if (awsCredentialsProvider != null) {
                            stsClientBuilder.credentialsProvider(awsCredentialsProvider);
                          }
                          return stsClientBuilder.build();
                        },
                        configuration.getGcpCredentialsProvider()))
                    .to(PolarisStorageIntegrationProvider.class);
                bind(new PolarisMetricRegistry(
                        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)))
                    .to(PolarisMetricRegistry.class);
                bind(openTelemetry).to(OpenTelemetry.class);
                bindAsContract(RealmEntityManagerFactory.class).in(RealmScope.class);
                bind(PolarisCallContextCatalogFactory.class)
                    .to(CallContextCatalogFactory.class)
                    .in(Singleton.class);
                bind(PolarisAuthorizerImpl.class).in(Singleton.class).to(PolarisAuthorizer.class);
                bind(IcebergCatalogAdapter.class)
                    .in(Singleton.class)
                    .to(IcebergRestCatalogApiService.class)
                    .to(IcebergRestConfigurationApiService.class);
                bind(PolarisServiceImpl.class)
                    .in(Singleton.class)
                    .to(PolarisCatalogsApiService.class)
                    .to(PolarisPrincipalsApiService.class)
                    .to(PolarisPrincipalRolesApiService.class);
                FileIOFactory fileIOFactory = configuration.getFileIOFactory();

                TaskHandlerConfiguration taskConfig = configuration.getTaskHandler();
                TaskExecutorImpl taskExecutor =
                    new TaskExecutorImpl(
                        taskConfig.executorService(), configuration.getMetaStoreManagerFactory());
                TaskFileIOSupplier fileIOSupplier =
                    new TaskFileIOSupplier(
                        configuration.getMetaStoreManagerFactory(), fileIOFactory);
                taskExecutor.addTaskHandler(
                    new TableCleanupTaskHandler(
                        taskExecutor, configuration.getMetaStoreManagerFactory(), fileIOSupplier));
                taskExecutor.addTaskHandler(
                    new ManifestFileCleanupTaskHandler(
                        fileIOSupplier, Executors.newVirtualThreadPerTaskExecutor()));

                bind(taskExecutor).to(TaskExecutor.class);
              }
            });
    MetaStoreManagerFactory metaStoreManagerFactory = configuration.getMetaStoreManagerFactory();
    environment
        .jersey()
        .register(
            new Feature() {
              @Inject ServiceLocator serviceLocator;

              @Override
              public boolean configure(FeatureContext context) {
                serviceLocator.inject(configuration.getMetaStoreManagerFactory());
                serviceLocator.inject(configuration.getConfigurationStore());
                serviceLocator.inject(configuration.getPolarisAuthenticator());
                serviceLocator.inject(configuration.getTokenBrokerFactory());
                serviceLocator.inject(configuration.getOauth2Service());
                serviceLocator.inject(configuration.getFileIOFactory());
                serviceLocator.inject(configuration.getCallContextResolver());
                serviceLocator.inject(configuration.getRealmContextResolver());
                serviceLocator.inject(configuration.getRateLimiter());
                return true;
              }
            });

    RealmContextResolver realmContextResolver = configuration.getRealmContextResolver();
    CallContextResolver callContextResolver = configuration.getCallContextResolver();
    environment
        .servlets()
        .addFilter(
            "realmContext", new ContextResolverFilter(realmContextResolver, callContextResolver))
        .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");

    LOGGER.info(
        "Initializing PolarisCallContextCatalogFactory for metaStoreManagerType {}",
        metaStoreManagerFactory);

    environment.jersey().register(IcebergRestCatalogApi.class);
    environment.jersey().register(IcebergRestConfigurationApi.class);

    FilterRegistration.Dynamic corsRegistration =
        environment.servlets().addFilter("CORS", CrossOriginFilter.class);
    corsRegistration.setInitParameter(
        CrossOriginFilter.ALLOWED_ORIGINS_PARAM,
        String.join(",", configuration.getCorsConfiguration().getAllowedOrigins()));
    corsRegistration.setInitParameter(
        CrossOriginFilter.ALLOWED_TIMING_ORIGINS_PARAM,
        String.join(",", configuration.getCorsConfiguration().getAllowedTimingOrigins()));
    corsRegistration.setInitParameter(
        CrossOriginFilter.ALLOWED_METHODS_PARAM,
        String.join(",", configuration.getCorsConfiguration().getAllowedMethods()));
    corsRegistration.setInitParameter(
        CrossOriginFilter.ALLOWED_HEADERS_PARAM,
        String.join(",", configuration.getCorsConfiguration().getAllowedHeaders()));
    corsRegistration.setInitParameter(
        CrossOriginFilter.ALLOW_CREDENTIALS_PARAM,
        String.join(",", configuration.getCorsConfiguration().getAllowCredentials()));
    corsRegistration.setInitParameter(
        CrossOriginFilter.PREFLIGHT_MAX_AGE_PARAM,
        Objects.toString(configuration.getCorsConfiguration().getPreflightMaxAge()));
    corsRegistration.setInitParameter(
        CrossOriginFilter.ALLOW_CREDENTIALS_PARAM,
        configuration.getCorsConfiguration().getAllowCredentials());
    corsRegistration.addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");
    //    environment
    //        .servlets()
    //        .addFilter("tracing", TracingFilter.class)
    //        .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");
    //
    //    if (configuration.getRateLimiter() != null) {
    //      environment
    //          .servlets()
    //          .addFilter("ratelimiter", RateLimiterFilter.class)
    //          .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");
    //    }

    environment
        .servlets()
        .addFilter("tracing", new TracingFilter(openTelemetry))
        .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");

    if (configuration.getRateLimiter() != null) {
      environment.jersey().register(new RateLimiterFilter(configuration.getRateLimiter()));
    }
    Authenticator<String, AuthenticatedPolarisPrincipal> authenticator =
        configuration.getPolarisAuthenticator();
    AuthFilter<String, AuthenticatedPolarisPrincipal> oauthCredentialAuthFilter =
        new OAuthCredentialAuthFilter.Builder<AuthenticatedPolarisPrincipal>()
            .setAuthenticator(authenticator)
            .setPrefix("Bearer")
            .buildAuthFilter();
    environment.jersey().register(new AuthDynamicFeature(oauthCredentialAuthFilter));
    environment.healthChecks().register("polaris", new PolarisHealthCheck());

    environment.jersey().register(IcebergRestOAuth2Api.class);
    environment.jersey().register(IcebergExceptionMapper.class);
    environment.jersey().register(PolarisExceptionMapper.class);

    environment.jersey().register(PolarisCatalogsApi.class);
    environment.jersey().register(PolarisPrincipalsApi.class);
    environment.jersey().register(PolarisPrincipalRolesApi.class);

    ObjectMapper objectMapper = environment.getObjectMapper();
    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());

    long maxRequestBodyBytes = configuration.getMaxRequestBodyBytes();
    if (maxRequestBodyBytes != REQUEST_BODY_BYTES_NO_LIMIT) {
      objectMapper
          .getFactory()
          .setStreamReadConstraints(
              StreamReadConstraints.builder().maxDocumentLength(maxRequestBodyBytes).build());
      LOGGER.info("Limiting request body size to {} bytes", maxRequestBodyBytes);
    }

    environment.jersey().register(StreamReadConstraintsExceptionMapper.class);
    RESTSerializers.registerAll(objectMapper);
    Serializers.registerSerializers(objectMapper);
    environment.jersey().register(IcebergJsonProcessingExceptionMapper.class);
    environment.jersey().register(IcebergJerseyViolationExceptionMapper.class);
    environment.jersey().register(TimedApplicationEventListener.class);

    environment
        .admin()
        .addServlet("metrics", PrometheusMetricsServlet.class)
        .addMapping("/metrics");

    // For in-memory metastore we need to bootstrap Service and Service principal at startup (for
    // default realm)
    // We can not utilize dropwizard Bootstrap command as command and server will be running two
    // different processes
    // and in-memory state will be lost b/w invocation of bootstrap command and running a server
    if (metaStoreManagerFactory instanceof InMemoryPolarisMetaStoreManagerFactory) {
      metaStoreManagerFactory.getOrCreateMetaStoreManager(configuration::getDefaultRealm);
    }

    LOGGER.info("Server started successfully.");
  }

  private static OpenTelemetry setupTracing() {
    Resource resource =
        Resource.getDefault().toBuilder()
            .put(ServiceAttributes.SERVICE_NAME, "polaris")
            .put(ServiceAttributes.SERVICE_VERSION, "0.1.0")
            .build();
    SdkTracerProvider sdkTracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(LoggingSpanExporter.create()))
            .setResource(resource)
            .build();
    return OpenTelemetrySdk.builder()
        .setTracerProvider(sdkTracerProvider)
        .setPropagators(
            ContextPropagators.create(
                TextMapPropagator.composite(
                    W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())))
        .build();
  }

  /** Resolves and sets ThreadLocal CallContext/RealmContext based on the request contents. */
  private static class ContextResolverFilter implements Filter {
    private final RealmContextResolver realmContextResolver;
    private final CallContextResolver callContextResolver;

    public ContextResolverFilter(
        RealmContextResolver realmContextResolver, CallContextResolver callContextResolver) {
      this.realmContextResolver = realmContextResolver;
      this.callContextResolver = callContextResolver;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
      HttpServletRequest httpRequest = (HttpServletRequest) request;
      Stream<String> headerNames = Collections.list(httpRequest.getHeaderNames()).stream();
      Map<String, String> headers =
          headerNames.collect(Collectors.toMap(Function.identity(), httpRequest::getHeader));
      RealmContext currentRealmContext =
          realmContextResolver.resolveRealmContext(
              httpRequest.getRequestURL().toString(),
              httpRequest.getMethod(),
              httpRequest.getRequestURI().substring(1),
              request.getParameterMap().entrySet().stream()
                  .collect(
                      Collectors.toMap(Map.Entry::getKey, (e) -> ((String[]) e.getValue())[0])),
              headers);
      CallContext currentCallContext =
          callContextResolver.resolveCallContext(
              currentRealmContext,
              httpRequest.getMethod(),
              httpRequest.getRequestURI().substring(1),
              request.getParameterMap().entrySet().stream()
                  .collect(
                      Collectors.toMap(Map.Entry::getKey, (e) -> ((String[]) e.getValue())[0])),
              headers);
      CallContext.setCurrentContext(currentCallContext);
      try (MDC.MDCCloseable ignored1 =
              MDC.putCloseable("realm", currentRealmContext.getRealmIdentifier());
          MDC.MDCCloseable ignored2 =
              MDC.putCloseable("request_id", httpRequest.getHeader("request_id"))) {
        chain.doFilter(request, response);
      } finally {
        currentCallContext.close();
      }
    }
  }

  private static class PolarisMetaStoreManagerFactory implements Factory<PolarisMetaStoreManager> {
    @Inject MetaStoreManagerFactory metaStoreManagerFactory;

    @RealmScope
    @Override
    public PolarisMetaStoreManager provide() {
      RealmContext realmContext = CallContext.getCurrentContext().getRealmContext();
      return metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    }

    @Override
    public void dispose(PolarisMetaStoreManager instance) {}
  }

  private static class PolarisRemoteCacheFactory implements Factory<PolarisRemoteCache> {
    @Inject PolarisMetaStoreManager metaStoreManager;

    @RealmScope
    @Override
    public PolarisRemoteCache provide() {
      return metaStoreManager;
    }

    @Override
    public void dispose(PolarisRemoteCache instance) {}
  }
}
