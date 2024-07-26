package io.polaris.service.auth;

import com.auth0.jwt.algorithms.Algorithm;
import io.polaris.core.persistence.PolarisEntityManager;
import java.util.function.Supplier;

/** Generates a JWT using a Symmetric Key. */
public class JWTSymmetricKeyBroker extends JWTBroker {
  private final Supplier<String> secretSupplier;

  JWTSymmetricKeyBroker(
      PolarisEntityManager entityManager,
      int maxTokenGenerationInSeconds,
      Supplier<String> secretSupplier) {
    super(entityManager, maxTokenGenerationInSeconds);
    this.secretSupplier = secretSupplier;
  }

  @Override
  Algorithm getAlgorithm() {
    return Algorithm.HMAC256(secretSupplier.get());
  }
}
