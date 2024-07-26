package io.polaris.service.auth;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.polaris.core.context.RealmContext;
import io.polaris.service.config.HasEntityManagerFactory;
import io.polaris.service.config.RealmEntityManagerFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Supplier;

@JsonTypeName("symmetric-key")
public class JWTSymmetricKeyFactory implements TokenBrokerFactory, HasEntityManagerFactory {
  private RealmEntityManagerFactory realmEntityManagerFactory;
  private int maxTokenGenerationInSeconds = 3600;
  private String file;
  private String secret;

  @Override
  public TokenBroker apply(RealmContext realmContext) {
    if (file == null && secret == null) {
      throw new IllegalStateException("Either file or secret must be set");
    }
    Supplier<String> secretSupplier = secret != null ? () -> secret : readSecretFromDisk();
    return new JWTSymmetricKeyBroker(
        realmEntityManagerFactory.getOrCreateEntityManager(realmContext),
        maxTokenGenerationInSeconds,
        secretSupplier);
  }

  private Supplier<String> readSecretFromDisk() {
    return () -> {
      try {
        return Files.readString(Paths.get(file));
      } catch (IOException e) {
        throw new RuntimeException("Failed to read secret from file: " + file, e);
      }
    };
  }

  public void setMaxTokenGenerationInSeconds(int maxTokenGenerationInSeconds) {
    this.maxTokenGenerationInSeconds = maxTokenGenerationInSeconds;
  }

  public void setFile(String file) {
    this.file = file;
  }

  public void setSecret(String secret) {
    this.secret = secret;
  }

  @Override
  public void setEntityManagerFactory(RealmEntityManagerFactory entityManagerFactory) {
    this.realmEntityManagerFactory = entityManagerFactory;
  }
}
