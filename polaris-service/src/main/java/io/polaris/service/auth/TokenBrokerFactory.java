package io.polaris.service.auth;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import io.polaris.core.context.RealmContext;
import java.util.function.Function;

/**
 * Factory that creates a {@link TokenBroker} for generating and parsing. The {@link TokenBroker} is
 * created based on the realm context.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface TokenBrokerFactory extends Function<RealmContext, TokenBroker>, Discoverable {}
