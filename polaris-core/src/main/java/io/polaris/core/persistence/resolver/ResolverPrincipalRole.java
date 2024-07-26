package io.polaris.core.persistence.resolver;

/** Expected principal type for the principal. Expectation depends on the REST request type */
public enum ResolverPrincipalRole {
  ANY_PRINCIPAL,
  CATALOG_ADMIN_PRINCIPAL,
  SERVICE_ADMIN_PRINCIPAL
}
