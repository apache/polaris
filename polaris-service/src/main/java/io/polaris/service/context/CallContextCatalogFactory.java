package io.polaris.service.context;

import io.polaris.core.context.CallContext;
import io.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.iceberg.catalog.Catalog;

public interface CallContextCatalogFactory {
  Catalog createCallContextCatalog(CallContext context, PolarisResolutionManifest resolvedManifest);
}
