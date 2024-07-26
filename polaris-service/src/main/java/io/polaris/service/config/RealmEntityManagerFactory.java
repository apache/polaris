package io.polaris.service.config;

import io.polaris.core.context.RealmContext;
import io.polaris.core.persistence.MetaStoreManagerFactory;
import io.polaris.core.persistence.PolarisEntityManager;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gets or creates PolarisEntityManager instances based on config values and RealmContext. */
public class RealmEntityManagerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(RealmEntityManagerFactory.class);
  private final MetaStoreManagerFactory metaStoreManagerFactory;

  // Key: realmIdentifier
  private Map<String, PolarisEntityManager> cachedEntityManagers = new HashMap<>();

  // Subclasses for test injection.
  protected RealmEntityManagerFactory() {
    this.metaStoreManagerFactory = null;
  }

  public RealmEntityManagerFactory(MetaStoreManagerFactory metaStoreManagerFactory) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  public PolarisEntityManager getOrCreateEntityManager(RealmContext context) {
    String realm = context.getRealmIdentifier();

    LOG.debug("Looking up PolarisEntityManager for realm {}", realm);
    PolarisEntityManager entityManagerInstance = cachedEntityManagers.get(realm);
    if (entityManagerInstance == null) {
      LOG.info("Initializing new PolarisEntityManager for realm {}", realm);

      entityManagerInstance =
          new PolarisEntityManager(
              metaStoreManagerFactory.getOrCreateMetaStoreManager(context),
              metaStoreManagerFactory.getOrCreateSessionSupplier(context),
              metaStoreManagerFactory.getOrCreateStorageCredentialCache(context));

      cachedEntityManagers.put(realm, entityManagerInstance);
    }
    return entityManagerInstance;
  }
}
