/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.persistence.relational.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JdbcDataSourceFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDataSourceFactory.class);

  private static final String DEFAULT_DRIVER_DIRECTORY = "jdbc-drivers";
  private static final String POOL_NAME = "polaris-relational-jdbc";

  private JdbcDataSourceFactory() {}

  static Optional<DataSource> create(RelationalJdbcConfiguration configuration) {
    Optional<String> jdbcUrl = nonBlank(configuration.jdbcUrl());
    if (jdbcUrl.isEmpty()) {
      return Optional.empty();
    }

    Optional<URLClassLoader> driverClassLoader = createDriverClassLoader(configuration);
    if (driverClassLoader.isPresent()) {
      return Optional.of(
          createHikariDataSource(configuration, jdbcUrl.get(), driverClassLoader.get()));
    }

    return Optional.of(new HikariDataSource(createHikariConfig(configuration, jdbcUrl.get())));
  }

  private static HikariDataSource createHikariDataSource(
      RelationalJdbcConfiguration configuration, String jdbcUrl, URLClassLoader driverClassLoader) {
    ClassLoader previousContextClassLoader = Thread.currentThread().getContextClassLoader();
    boolean success = false;
    try {
      Thread.currentThread().setContextClassLoader(driverClassLoader);
      HikariDataSource dataSource =
          new DriverClassLoaderHikariDataSource(
              createHikariConfig(configuration, jdbcUrl), driverClassLoader);
      success = true;
      return dataSource;
    } finally {
      Thread.currentThread().setContextClassLoader(previousContextClassLoader);
      if (!success) {
        closeDriverClassLoader(driverClassLoader);
      }
    }
  }

  private static HikariConfig createHikariConfig(
      RelationalJdbcConfiguration configuration, String jdbcUrl) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setPoolName(POOL_NAME);
    hikariConfig.setJdbcUrl(jdbcUrl);
    nonBlank(configuration.driver()).ifPresent(hikariConfig::setDriverClassName);
    nonBlank(configuration.username()).ifPresent(hikariConfig::setUsername);
    configuration.password().ifPresent(hikariConfig::setPassword);
    configuration.maximumPoolSize().ifPresent(hikariConfig::setMaximumPoolSize);
    configuration.minimumIdle().ifPresent(hikariConfig::setMinimumIdle);
    configuration.connectionTimeoutInMs().ifPresent(hikariConfig::setConnectionTimeout);

    return hikariConfig;
  }

  private static Optional<URLClassLoader> createDriverClassLoader(
      RelationalJdbcConfiguration configuration) {
    Optional<String> configuredDriverDirectory = nonBlank(configuration.driverDirectory());
    Optional<Path> driverDirectory =
        configuredDriverDirectory
            .map(Path::of)
            .or(() -> defaultDriverDirectory().filter(Files::isDirectory));

    if (driverDirectory.isEmpty()) {
      return Optional.empty();
    }

    Path directory = driverDirectory.get();
    if (!Files.isDirectory(directory)) {
      throw new IllegalArgumentException(
          "Configured JDBC driver directory does not exist: " + directory);
    }

    List<Path> driverJars = driverJars(directory);
    if (driverJars.isEmpty()) {
      if (configuredDriverDirectory.isPresent()) {
        throw new IllegalArgumentException(
            "Configured JDBC driver directory does not contain any .jar files: " + directory);
      }
      return Optional.empty();
    }

    LOGGER.info("Loading JDBC driver jars from {}", directory);
    URL[] urls = driverJars.stream().map(JdbcDataSourceFactory::toUrl).toArray(URL[]::new);
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    if (parent == null) {
      parent = JdbcDataSourceFactory.class.getClassLoader();
    }
    return Optional.of(new URLClassLoader(urls, parent));
  }

  private static Optional<Path> defaultDriverDirectory() {
    String classPath = System.getProperty("java.class.path", "");
    for (String classPathEntry : classPath.split(System.getProperty("path.separator"))) {
      if (classPathEntry.isEmpty()) {
        continue;
      }
      Path path = Path.of(classPathEntry);
      Path fileName = path.getFileName();
      if (fileName != null && "quarkus-run.jar".equals(fileName.toString())) {
        Path parent = path.getParent();
        return Optional.of(
            parent == null
                ? Path.of(DEFAULT_DRIVER_DIRECTORY)
                : parent.resolve(DEFAULT_DRIVER_DIRECTORY));
      }
    }
    return Optional.empty();
  }

  private static List<Path> driverJars(Path driverDirectory) {
    try (Stream<Path> entries = Files.list(driverDirectory)) {
      return entries
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".jar"))
          .sorted(Comparator.comparing(path -> path.getFileName().toString()))
          .toList();
    } catch (IOException e) {
      throw new RuntimeException("Failed to list JDBC driver directory: " + driverDirectory, e);
    }
  }

  private static URL toUrl(Path driverJar) {
    try {
      return driverJar.toUri().toURL();
    } catch (IOException e) {
      throw new RuntimeException("Failed to load JDBC driver jar: " + driverJar, e);
    }
  }

  private static void closeDriverClassLoader(URLClassLoader driverClassLoader) {
    try {
      driverClassLoader.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to close JDBC driver classloader", e);
    }
  }

  private static Optional<String> nonBlank(Optional<String> value) {
    return value.map(String::trim).filter(s -> !s.isEmpty());
  }

  private static final class DriverClassLoaderHikariDataSource extends HikariDataSource {
    private final URLClassLoader driverClassLoader;

    private DriverClassLoaderHikariDataSource(
        HikariConfig configuration, URLClassLoader driverClassLoader) {
      super(configuration);
      this.driverClassLoader = driverClassLoader;
    }

    @Override
    public void close() {
      RuntimeException closeFailure = null;
      try {
        super.close();
      } catch (RuntimeException e) {
        closeFailure = e;
      }

      try {
        driverClassLoader.close();
      } catch (IOException e) {
        RuntimeException classLoaderFailure =
            new RuntimeException("Failed to close JDBC driver classloader", e);
        if (closeFailure != null) {
          closeFailure.addSuppressed(classLoaderFailure);
        } else {
          closeFailure = classLoaderFailure;
        }
      }

      if (closeFailure != null) {
        throw closeFailure;
      }
    }
  }
}
