package io.polaris.service.auth;

import com.auth0.jwt.algorithms.Algorithm;
import io.polaris.core.persistence.PolarisEntityManager;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

/** Generates a JWT using a Public/Private RSA Key */
public class JWTRSAKeyPair extends JWTBroker {

  JWTRSAKeyPair(PolarisEntityManager entityManager, int maxTokenGenerationInSeconds) {
    super(entityManager, maxTokenGenerationInSeconds);
  }

  KeyProvider getKeyProvider() {
    return new LocalRSAKeyProvider();
  }

  @Override
  Algorithm getAlgorithm() {
    KeyProvider keyProvider = getKeyProvider();
    return Algorithm.RSA256(
        (RSAPublicKey) keyProvider.getPublicKey(), (RSAPrivateKey) keyProvider.getPrivateKey());
  }
}
