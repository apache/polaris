package io.polaris.service.catalog;

import io.polaris.core.storage.PolarisStorageActions;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Adds support for credential vending for (typically) {@link org.apache.iceberg.TableOperations} to
 * fetch access credentials that are inserted into the {@link
 * org.apache.iceberg.rest.responses.LoadTableResponse#config} property. See the
 * rest-catalog-open-api.yaml spec for details on the expected format of vended credential
 * configuration.
 */
public interface SupportsCredentialDelegation {
  Map<String, String> getCredentialConfig(
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata,
      Set<PolarisStorageActions> storageActions);
}
