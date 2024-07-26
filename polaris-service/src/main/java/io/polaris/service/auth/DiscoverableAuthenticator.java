package io.polaris.service.auth;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.jackson.Discoverable;
import io.polaris.service.config.HasEntityManagerFactory;
import java.security.Principal;

/**
 * Extension of the {@link Authenticator} interface that extends {@link Discoverable} so
 * implementations can be discovered using the mechanisms described in
 * https://www.dropwizard.io/en/stable/manual/configuration.html#polymorphic-configuration . The
 * default implementation is {@link TestInlineBearerTokenPolarisAuthenticator}.
 *
 * @param <C>
 * @param <P>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
public interface DiscoverableAuthenticator<C, P extends Principal>
    extends Authenticator<C, P>, Discoverable, HasEntityManagerFactory {}
