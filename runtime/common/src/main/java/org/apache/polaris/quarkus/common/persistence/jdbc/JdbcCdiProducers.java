package org.apache.polaris.quarkus.common.persistence.jdbc;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.persistence.relational.jdbc.DataSourceResolver;
import org.apache.polaris.quarkus.common.config.jdbc.QuarkusRelationalJdbcConfiguration;

/**
 * CDI producers for JDBC-specific beans. Moved to runtime-common to keep the persistence layer
 * implementation-agnostic regarding configuration sources.
 */
public class JdbcCdiProducers {

  /**
   * Produces the active {@link DataSourceResolver} by selecting the bean identified by {@link
   * QuarkusRelationalJdbcConfiguration#dataSourceResolverType()}.
   *
   * <p>The result is {@link ApplicationScoped} because the datasource-resolver type cannot change
   * at runtime.
   */
  @Produces
  @ApplicationScoped
  public DataSourceResolver dataSourceResolver(
      QuarkusRelationalJdbcConfiguration jdbcConfig,
      @Any Instance<DataSourceResolver> dataSourceResolvers) {
    String type = jdbcConfig.dataSourceResolverType();
    return dataSourceResolvers.select(Identifier.Literal.of(type)).get();
  }
}
