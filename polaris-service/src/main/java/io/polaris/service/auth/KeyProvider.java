package io.polaris.service.auth;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import java.security.PrivateKey;
import java.security.PublicKey;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface KeyProvider extends Discoverable {
  PublicKey getPublicKey();

  PrivateKey getPrivateKey();
}
