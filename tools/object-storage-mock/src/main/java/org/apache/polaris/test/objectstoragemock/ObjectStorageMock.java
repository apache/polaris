/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// CODE_COPIED_TO_POLARIS from Project Nessie 0.106.1
package org.apache.polaris.test.objectstoragemock;

import com.fasterxml.jackson.jakarta.rs.xml.JacksonXMLProvider;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseFilter;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.test.objectstoragemock.sts.AssumeRoleHandler;
import org.apache.polaris.test.objectstoragemock.sts.ImmutableAssumeRoleResult;
import org.apache.polaris.test.objectstoragemock.sts.ImmutableCredentials;
import org.apache.polaris.test.objectstoragemock.sts.ImmutableRoleUser;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public abstract class ObjectStorageMock {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageMock.class);

  /**
   * The hostname with which to create the HTTP server. The default is 127.0.0.1.
   *
   * <p>Note: for S3, the default address will generate endpoint URIs that work with S3 clients out
   * of the box, but technically, they are not valid S3 endpoints. If you need compliance, for
   * example to make the endpoint URI parseable by {@code S3Utilities}, use {@code
   * s3.127-0-0-1.nip.io} instead. Make sure in this case to create your S3 clients with path-style
   * access enforced, because if the endpoint is valid, the client will attempt to use
   * virtual-host-style access by default, which this S3 mock server cannot handle.
   */
  @Value.Default
  public String initAddress() {
    return "127.0.0.1";
  }

  public static ImmutableObjectStorageMock.Builder builder() {
    return ImmutableObjectStorageMock.builder();
  }

  public abstract Map<String, Bucket> buckets();

  @Value.Default
  public AssumeRoleHandler assumeRoleHandler() {
    return ((action,
        version,
        roleArn,
        roleSessionName,
        policy,
        durationSeconds,
        externalId,
        serialNumber) ->
        ImmutableAssumeRoleResult.builder()
            .credentials(
                ImmutableCredentials.builder()
                    .accessKeyId("access-key-id")
                    .secretAccessKey("secret-access-key")
                    .expiration(Instant.now().plus(15, ChronoUnit.MINUTES))
                    .build())
            .sourceIdentity("source-identity")
            .assumedRoleUser(
                ImmutableRoleUser.builder().arn("arn").assumedRoleId("assumedRoleId").build())
            .build());
  }

  @Value.Default
  public AccessCheckHandler accessCheckHandler() {
    return (key) -> true;
  }

  public interface MockServer extends AutoCloseable {
    URI getS3BaseUri();

    URI getGcsBaseUri();

    URI getAdlsGen2BaseUri();

    URI getStsEndpointURI();

    default Map<String, String> icebergProperties() {
      Map<String, String> props = new HashMap<>();
      props.put("s3.access-key-id", "accessKey");
      props.put("s3.secret-access-key", "secretKey");
      props.put("s3.endpoint", getS3BaseUri().toString());
      // must enforce path-style access because S3Resource has the bucket name in its path
      props.put("s3.path-style-access", "true");
      props.put("http-client.type", "urlconnection");
      return props;
    }
  }

  private static final class MockServerImpl implements MockServer {

    private final Server server;

    private final URI baseUri;

    public MockServerImpl(URI initUri, ResourceConfig config) {
      this.server = JettyHttpContainerFactory.createServer(initUri, config, true);
      customizeUriCompliance();
      this.baseUri = baseUri(server, initUri);

      LOGGER.info("Object storage mock started started at {}", baseUri);
    }

    /**
     * Allows ambiguous path separators, because Microsoft's Azure clients do send URL-encoded path
     * separators, so {@code /} as {@code %2f}, which are considered to be insecure and is (should
     * be) rejected by containers since <a
     * href="https://github.com/jakartaee/servlet/blob/6.0.0-RELEASE/spec/src/main/asciidoc/servlet-spec-body.adoc#352-uri-path-canonicalization">service
     * spec v6</a>.
     */
    private void customizeUriCompliance() {
      for (Connector connector : server.getConnectors()) {
        connector.getConnectionFactories().stream()
            .filter(factory -> factory instanceof HttpConnectionFactory)
            .forEach(
                factory -> {
                  HttpConfiguration httpConfig =
                      ((HttpConnectionFactory) factory).getHttpConfiguration();
                  httpConfig.setUriCompliance(
                      UriCompliance.from(Set.of(UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR)));
                });
      }
    }

    private static URI baseUri(Server server, URI initUri) {
      for (Connector connector : server.getConnectors()) {
        if (connector instanceof ServerConnector sc) {
          int localPort = sc.getLocalPort();
          try {
            return new URI(
                initUri.getScheme(),
                initUri.getUserInfo(),
                initUri.getHost(),
                localPort,
                initUri.getPath(),
                null,
                null);
          } catch (URISyntaxException e) {
            throw new RuntimeException(e);
          }
        }
      }

      throw new IllegalArgumentException("Server has no connectors");
    }

    @Override
    public URI getStsEndpointURI() {
      return baseUri.resolve("sts/assumeRole");
    }

    @Override
    public URI getS3BaseUri() {
      return baseUri;
    }

    @Override
    public URI getGcsBaseUri() {
      return baseUri;
    }

    @Override
    public URI getAdlsGen2BaseUri() {
      return baseUri.resolve("adlsgen2/");
    }

    @Override
    public void close() throws Exception {
      if (server != null) {
        LOGGER.info("Stopping object storage mock server at {}", baseUri);

        server.stop();
      }
    }
  }

  public MockServer start() {
    ResourceConfig config = new ResourceConfig();
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            this.bind(ObjectStorageMock.this).to(ObjectStorageMock.class);
          }
        });
    config.register(JacksonXMLProvider.class);

    config.register(S3Resource.class);
    config.register(AdlsGen2Resource.class);
    config.register(GcsResource.class);
    config.register(StsResource.class);

    if (LOGGER.isDebugEnabled()) {
      config.register(
          (ContainerRequestFilter)
              requestContext -> {
                LOGGER.debug(
                    "{} {} {}",
                    requestContext.getMethod(),
                    requestContext.getUriInfo().getPath(),
                    requestContext.getUriInfo().getRequestUri().getQuery());
                requestContext.getHeaders().forEach((k, v) -> LOGGER.debug("  {}: {}", k, v));
              });
      config.register(
          (ContainerResponseFilter)
              (requestContext, responseContext) -> {
                LOGGER.debug("{}", responseContext.getStatusInfo());
                responseContext.getHeaders().forEach((k, v) -> LOGGER.debug("  {}: {}", k, v));
              });
    }

    URI initUri = URI.create(String.format("http://%s:0/", initAddress()));

    return new MockServerImpl(initUri, config);
  }
}
