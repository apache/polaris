package io.polaris.service.auth;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.polaris.core.context.RealmContext;
import io.polaris.service.config.HasEntityManagerFactory;
import io.polaris.service.config.RealmEntityManagerFactory;

@JsonTypeName("rsa-key-pair")
public class JWTRSAKeyPairFactory implements TokenBrokerFactory, HasEntityManagerFactory {
  private int maxTokenGenerationInSeconds = 3600;
  private RealmEntityManagerFactory realmEntityManagerFactory;

  public void setMaxTokenGenerationInSeconds(int maxTokenGenerationInSeconds) {
    this.maxTokenGenerationInSeconds = maxTokenGenerationInSeconds;
  }

  @Override
  public TokenBroker apply(RealmContext realmContext) {
    return new JWTRSAKeyPair(
        realmEntityManagerFactory.getOrCreateEntityManager(realmContext),
        maxTokenGenerationInSeconds);
  }

  @Override
  public void setEntityManagerFactory(RealmEntityManagerFactory entityManagerFactory) {
    this.realmEntityManagerFactory = entityManagerFactory;
  }
}
