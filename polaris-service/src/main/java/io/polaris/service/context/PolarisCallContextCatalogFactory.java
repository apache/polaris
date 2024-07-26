package io.polaris.service.context;

import io.polaris.core.context.CallContext;
import io.polaris.core.entity.CatalogEntity;
import io.polaris.core.entity.PolarisBaseEntity;
import io.polaris.core.persistence.PolarisEntityManager;
import io.polaris.core.persistence.resolver.PolarisResolutionManifest;
import io.polaris.service.catalog.BasePolarisCatalog;
import io.polaris.service.config.RealmEntityManagerFactory;
import io.polaris.service.task.TaskExecutor;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolarisCallContextCatalogFactory implements CallContextCatalogFactory {
  private static final Logger LOG = LoggerFactory.getLogger(PolarisCallContextCatalogFactory.class);

  private static final String WAREHOUSE_LOCATION_BASEDIR =
      "/tmp/iceberg_rest_server_warehouse_data/";

  private final RealmEntityManagerFactory entityManagerFactory;
  private final TaskExecutor taskExecutor;

  public PolarisCallContextCatalogFactory(
      RealmEntityManagerFactory entityManagerFactory, TaskExecutor taskExecutor) {
    this.entityManagerFactory = entityManagerFactory;
    this.taskExecutor = taskExecutor;
  }

  @Override
  public Catalog createCallContextCatalog(
      CallContext context, final PolarisResolutionManifest resolvedManifest) {
    PolarisBaseEntity baseCatalogEntity =
        resolvedManifest.getResolvedReferenceCatalogEntity().getRawLeafEntity();
    String catalogName = baseCatalogEntity.getName();

    String realm = context.getRealmContext().getRealmIdentifier();
    String catalogKey = realm + "/" + catalogName;
    LOG.info("Initializing new BasePolarisCatalog for key: {}", catalogKey);

    PolarisEntityManager entityManager =
        entityManagerFactory.getOrCreateEntityManager(context.getRealmContext());

    BasePolarisCatalog catalogInstance =
        new BasePolarisCatalog(entityManager, context, resolvedManifest, taskExecutor);

    context.contextVariables().put(CallContext.REQUEST_PATH_CATALOG_INSTANCE_KEY, catalogInstance);

    CatalogEntity catalog = CatalogEntity.of(baseCatalogEntity);
    Map<String, String> catalogProperties = new HashMap<>(catalog.getPropertiesAsMap());
    String defaultBaseLocation = catalog.getDefaultBaseLocation();
    LOG.info("Looked up defaultBaseLocation {} for catalog {}", defaultBaseLocation, catalogKey);
    if (defaultBaseLocation != null) {
      catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, defaultBaseLocation);
    } else {
      catalogProperties.put(
          CatalogProperties.WAREHOUSE_LOCATION,
          Paths.get(WAREHOUSE_LOCATION_BASEDIR, catalogKey).toString());
    }

    // TODO: The initialize properties might need to take more from CallContext and the
    // CatalogEntity.
    catalogInstance.initialize(catalogName, catalogProperties);

    return catalogInstance;
  }
}
