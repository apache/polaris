package io.polaris.service.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.polaris.core.auth.AuthenticatedPolarisPrincipal;
import io.polaris.core.context.CallContext;
import io.polaris.service.config.HasEntityManagerFactory;
import io.polaris.service.config.RealmEntityManagerFactory;
import java.util.Optional;

public class DefaultPolarisAuthenticator extends BasePolarisAuthenticator {
  private TokenBrokerFactory tokenBrokerFactory;

  @Override
  public Optional<AuthenticatedPolarisPrincipal> authenticate(String credentials) {
    TokenBroker handler =
        tokenBrokerFactory.apply(CallContext.getCurrentContext().getRealmContext());
    DecodedToken decodedToken = handler.verify(credentials);
    return getPrincipal(decodedToken);
  }

  @Override
  public void setEntityManagerFactory(RealmEntityManagerFactory entityManagerFactory) {
    super.setEntityManagerFactory(entityManagerFactory);
    if (tokenBrokerFactory instanceof HasEntityManagerFactory) {
      ((HasEntityManagerFactory) tokenBrokerFactory).setEntityManagerFactory(entityManagerFactory);
    }
  }

  @JsonProperty("tokenBroker")
  public void setTokenBroker(TokenBrokerFactory tokenBrokerFactory) {
    this.tokenBrokerFactory = tokenBrokerFactory;
  }
}
