package io.polaris.service.auth;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.context.CallContext;
import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that can load public / private keys stored on localhost. Meant to be a simple
 * implementation for now where a PEM file is loaded off disk.
 */
public class LocalRSAKeyProvider implements KeyProvider {

  private static final String LOCAL_PRIVATE_KEY_LOCATION_KEY = "LOCAL_PRIVATE_KEY_LOCATION_KEY";
  private static final String LOCAL_PUBLIC_KEY_LOCATION_KEY = "LOCAL_PUBLIC_LOCATION_KEY";

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalRSAKeyProvider.class);

  private String getLocation(String configKey) {
    CallContext callContext = CallContext.getCurrentContext();
    PolarisCallContext pCtx = callContext.getPolarisCallContext();
    String fileLocation = pCtx.getConfigurationStore().getConfiguration(pCtx, configKey);
    if (fileLocation == null) {
      throw new RuntimeException("Cannot find location for key " + configKey);
    }
    return fileLocation;
  }

  /**
   * Getter for the Public Key instance
   *
   * @return the Public Key instance
   */
  @Override
  public PublicKey getPublicKey() {
    final String publicKeyFileLocation = getLocation(LOCAL_PUBLIC_KEY_LOCATION_KEY);
    try {
      return PemUtils.readPublicKeyFromFile(publicKeyFileLocation, "RSA");
    } catch (IOException e) {
      LOGGER.error("Unable to read public key from file {}", publicKeyFileLocation, e);
      throw new RuntimeException("Unable to read public key from file " + publicKeyFileLocation, e);
    }
  }

  /**
   * Getter for the Private Key instance. Used to sign the content on the JWT signing stage.
   *
   * @return the Private Key instance
   */
  @Override
  public PrivateKey getPrivateKey() {
    final String privateKeyFileLocation = getLocation(LOCAL_PRIVATE_KEY_LOCATION_KEY);
    try {
      return PemUtils.readPrivateKeyFromFile(privateKeyFileLocation, "RSA");
    } catch (IOException e) {
      LOGGER.error("Unable to read private key from file {}", privateKeyFileLocation, e);
      throw new RuntimeException(
          "Unable to read private key from file " + privateKeyFileLocation, e);
    }
  }
}
