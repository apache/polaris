package io.polaris.service.config;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import io.polaris.service.auth.TokenBrokerFactory;
import io.polaris.service.catalog.api.IcebergRestOAuth2ApiService;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface OAuth2ApiService extends Discoverable, IcebergRestOAuth2ApiService {
  void setTokenBroker(TokenBrokerFactory tokenBrokerFactory);
}
