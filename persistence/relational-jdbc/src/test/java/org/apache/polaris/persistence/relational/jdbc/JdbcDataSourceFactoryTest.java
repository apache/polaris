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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariDataSource;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.sql.DataSource;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JdbcDataSourceFactoryTest {

  @Test
  void createReturnsEmptyWhenJdbcUrlIsAbsent() {
    RelationalJdbcConfiguration configuration =
        TestingRelationalJdbcConfiguration.builder().build();

    assertThat(JdbcDataSourceFactory.create(configuration)).isEmpty();
  }

  @Test
  void createBuildsHikariDataSourceFromRuntimeConfig() throws Exception {
    String jdbcUrl = "jdbc:h2:mem:runtime_config_poc;DB_CLOSE_DELAY=-1";
    RelationalJdbcConfiguration configuration =
        TestingRelationalJdbcConfiguration.builder()
            .jdbcUrl(jdbcUrl)
            .driver("org.h2.Driver")
            .username("sa")
            .password("")
            .maximumPoolSize(2)
            .minimumIdle(1)
            .connectionTimeoutInMs(1000L)
            .build();

    DataSource dataSource = JdbcDataSourceFactory.create(configuration).orElseThrow();
    try {
      assertThat(dataSource).isInstanceOf(HikariDataSource.class);
      HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
      assertThat(hikariDataSource.getJdbcUrl()).isEqualTo(jdbcUrl);
      assertThat(hikariDataSource.getMaximumPoolSize()).isEqualTo(2);
      assertThat(hikariDataSource.getMinimumIdle()).isEqualTo(1);
      assertThat(hikariDataSource.getConnectionTimeout()).isEqualTo(1000L);

      try (Connection connection = dataSource.getConnection()) {
        assertThat(connection.getMetaData().getDatabaseProductName()).isEqualTo("H2");
      }
    } finally {
      ((HikariDataSource) dataSource).close();
    }
  }

  @Test
  void createBuildsIndependentDataSourcesForDifferentRealmConfigurations() throws Exception {
    HikariDataSource engineeringDataSource =
        createDataSource("jdbc:h2:mem:engineering_realm_poc;DB_CLOSE_DELAY=-1");
    HikariDataSource financeDataSource =
        createDataSource("jdbc:h2:mem:finance_realm_poc;DB_CLOSE_DELAY=-1");

    try {
      writeRealmMarker(engineeringDataSource, "engineering");
      writeRealmMarker(financeDataSource, "finance");

      assertThat(readRealmMarker(engineeringDataSource)).isEqualTo("engineering");
      assertThat(readRealmMarker(financeDataSource)).isEqualTo("finance");
    } finally {
      engineeringDataSource.close();
      financeDataSource.close();
    }
  }

  @Test
  void createUsesJdbcDriverLoadedFromRuntimeJar(@TempDir Path tempDir) throws Exception {
    String driverClassName = "runtime.jdbc.RuntimeLoadedJdbcDriver";
    Path driverDirectory = tempDir.resolve("jdbc-drivers");
    Files.createDirectories(driverDirectory);
    createRuntimeDriverJar(driverDirectory, driverClassName);

    assertThatThrownBy(
            () ->
                Class.forName(
                    driverClassName, false, JdbcDataSourceFactoryTest.class.getClassLoader()))
        .isInstanceOf(ClassNotFoundException.class);

    HikariDataSource dataSource =
        createDataSource(
            "jdbc:polaris-runtime-h2:mem:runtime_loaded_driver_poc;DB_CLOSE_DELAY=-1",
            driverClassName,
            driverDirectory);

    try {
      writeRealmMarker(dataSource, "runtime-loaded-driver");

      assertThat(readRealmMarker(dataSource)).isEqualTo("runtime-loaded-driver");
    } finally {
      dataSource.close();
    }
  }

  @Test
  void createUsesJdbcDriverLoadedFromDefaultRuntimeDirectory(@TempDir Path tempDir)
      throws Exception {
    String driverClassName = "runtime.jdbc.DefaultRuntimeLoadedJdbcDriver";
    Path serverDirectory = tempDir.resolve("server");
    Path launcherJar = serverDirectory.resolve("quarkus-run.jar");
    Path driverDirectory = serverDirectory.resolve("jdbc-drivers");
    Files.createDirectories(driverDirectory);
    Files.createFile(launcherJar);
    createRuntimeDriverJar(driverDirectory, driverClassName);

    String previousClassPath = System.getProperty("java.class.path");
    try {
      System.setProperty("java.class.path", launcherJar.toString());
      HikariDataSource dataSource =
          createDataSource(
              "jdbc:polaris-runtime-h2:mem:default_runtime_loaded_driver_poc;DB_CLOSE_DELAY=-1",
              driverClassName,
              null);

      try {
        writeRealmMarker(dataSource, "default-runtime-loaded-driver");

        assertThat(readRealmMarker(dataSource)).isEqualTo("default-runtime-loaded-driver");
      } finally {
        dataSource.close();
      }
    } finally {
      System.setProperty("java.class.path", previousClassPath);
    }
  }

  @Test
  void createRejectsConfiguredDriverDirectoryWithoutJars(@TempDir Path tempDir) throws Exception {
    Path driverDirectory = tempDir.resolve("jdbc-drivers");
    Files.createDirectories(driverDirectory);
    RelationalJdbcConfiguration configuration =
        TestingRelationalJdbcConfiguration.builder()
            .jdbcUrl("jdbc:h2:mem:missing_runtime_driver;DB_CLOSE_DELAY=-1")
            .driver("missing.Driver")
            .driverDirectory(driverDirectory.toString())
            .build();

    assertThatThrownBy(() -> JdbcDataSourceFactory.create(configuration))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not contain any .jar files");
  }

  private static HikariDataSource createDataSource(String jdbcUrl) {
    return createDataSource(jdbcUrl, "org.h2.Driver", null);
  }

  private static HikariDataSource createDataSource(
      String jdbcUrl, String driverClassName, Path driverDirectory) {
    TestingRelationalJdbcConfiguration.Builder builder =
        TestingRelationalJdbcConfiguration.builder()
            .jdbcUrl(jdbcUrl)
            .driver(driverClassName)
            .username("sa")
            .password("")
            .maximumPoolSize(1);
    if (driverDirectory != null) {
      builder.driverDirectory(driverDirectory.toString());
    }
    RelationalJdbcConfiguration configuration = builder.build();
    return (HikariDataSource) JdbcDataSourceFactory.create(configuration).orElseThrow();
  }

  private static void writeRealmMarker(DataSource dataSource, String realmId) throws Exception {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate("CREATE TABLE realm_marker (realm_id VARCHAR(255))");
      }
      try (PreparedStatement statement =
          connection.prepareStatement("INSERT INTO realm_marker VALUES (?)")) {
        statement.setString(1, realmId);
        statement.executeUpdate();
      }
      connection.commit();
    }
  }

  private static String readRealmMarker(DataSource dataSource) throws Exception {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT realm_id FROM realm_marker")) {
      assertThat(resultSet.next()).isTrue();
      String realmId = resultSet.getString(1);
      assertThat(resultSet.next()).isFalse();
      return realmId;
    }
  }

  private static Path createRuntimeDriverJar(Path tempDir, String driverClassName)
      throws Exception {
    Path sourceRoot = tempDir.resolve("source");
    Path classesRoot = tempDir.resolve("classes");
    Path sourceFile =
        sourceRoot.resolve(driverClassName.replace('.', File.separatorChar) + ".java");
    Files.createDirectories(sourceFile.getParent());
    Files.createDirectories(classesRoot);
    Files.writeString(sourceFile, runtimeDriverSource(driverClassName));

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    assertThat(compiler).isNotNull();
    int result =
        compiler.run(null, null, null, "-d", classesRoot.toString(), sourceFile.toString());
    assertThat(result).isZero();

    Path jarFile = tempDir.resolve("runtime-loaded-jdbc-driver.jar");
    try (JarOutputStream jarOutputStream = new JarOutputStream(Files.newOutputStream(jarFile))) {
      for (Path classFile : Files.walk(classesRoot).filter(Files::isRegularFile).toList()) {
        JarEntry entry =
            new JarEntry(
                classesRoot.relativize(classFile).toString().replace(File.separatorChar, '/'));
        jarOutputStream.putNextEntry(entry);
        Files.copy(classFile, jarOutputStream);
        jarOutputStream.closeEntry();
      }
    }
    return jarFile;
  }

  private static String runtimeDriverSource(String driverClassName) {
    int packageSeparator = driverClassName.lastIndexOf('.');
    String packageName = driverClassName.substring(0, packageSeparator);
    String simpleName = driverClassName.substring(packageSeparator + 1);
    return """
        package %s;

        import java.sql.Connection;
        import java.sql.Driver;
        import java.sql.DriverManager;
        import java.sql.DriverPropertyInfo;
        import java.sql.SQLException;
        import java.util.Properties;
        import java.util.logging.Logger;

        public final class %s implements Driver {
          private static final String URL_PREFIX = "jdbc:polaris-runtime-h2:";

          static {
            try {
              DriverManager.registerDriver(new %s());
            } catch (SQLException e) {
              throw new ExceptionInInitializerError(e);
            }
          }

          @Override
          public Connection connect(String url, Properties info) throws SQLException {
            if (!acceptsURL(url)) {
              return null;
            }
            try {
              Class.forName("org.h2.Driver");
            } catch (ClassNotFoundException e) {
              throw new SQLException(e);
            }
            return DriverManager.getConnection("jdbc:h2:" + url.substring(URL_PREFIX.length()), info);
          }

          @Override
          public boolean acceptsURL(String url) {
            return url != null && url.startsWith(URL_PREFIX);
          }

          @Override
          public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
          }

          @Override
          public int getMajorVersion() {
            return 1;
          }

          @Override
          public int getMinorVersion() {
            return 0;
          }

          @Override
          public boolean jdbcCompliant() {
            return false;
          }

          @Override
          public Logger getParentLogger() {
            return Logger.getGlobal();
          }
        }
        """
        .formatted(packageName, simpleName, simpleName);
  }
}
