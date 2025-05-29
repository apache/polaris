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
package org.apache.polaris.extension.persistence.impl.eclipselink;

import static org.eclipse.persistence.config.PersistenceUnitProperties.ECLIPSELINK_PERSISTENCE_XML;
import static org.eclipse.persistence.config.PersistenceUnitProperties.JDBC_URL;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.extension.persistence.impl.eclipselink.PolarisEclipseLinkPersistenceUnit.ClasspathResourcePolarisEclipseLinkPersistenceUnit;
import org.apache.polaris.extension.persistence.impl.eclipselink.PolarisEclipseLinkPersistenceUnit.FileSystemPolarisEclipseLinkPersistenceUnit;
import org.apache.polaris.extension.persistence.impl.eclipselink.PolarisEclipseLinkPersistenceUnit.JarFilePolarisEclipseLinkPersistenceUnit;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

sealed interface PolarisEclipseLinkPersistenceUnit
    permits ClasspathResourcePolarisEclipseLinkPersistenceUnit,
        FileSystemPolarisEclipseLinkPersistenceUnit,
        JarFilePolarisEclipseLinkPersistenceUnit {

  Map<String, String> loadProperties() throws IOException;

  EntityManagerFactory createEntityManagerFactory(@Nonnull RealmContext realmContext)
      throws IOException;

  record ClasspathResourcePolarisEclipseLinkPersistenceUnit(
      URL resource, String resourceName, String persistenceUnitName)
      implements PolarisEclipseLinkPersistenceUnit {

    @Override
    public Map<String, String> loadProperties() throws IOException {
      var properties = internalLoadProperties(resource, persistenceUnitName);
      properties.put(ECLIPSELINK_PERSISTENCE_XML, resourceName);
      return properties;
    }

    @Override
    public EntityManagerFactory createEntityManagerFactory(@Nonnull RealmContext realmContext)
        throws IOException {
      var properties = transformJdbcUrl(loadProperties(), realmContext);
      return Persistence.createEntityManagerFactory(persistenceUnitName, properties);
    }
  }

  record FileSystemPolarisEclipseLinkPersistenceUnit(Path path, String persistenceUnitName)
      implements PolarisEclipseLinkPersistenceUnit {

    @Override
    public Map<String, String> loadProperties() throws IOException {
      var properties = internalLoadProperties(path.toUri().toURL(), persistenceUnitName);
      Path archiveDirectory = path.getParent();
      String descriptorPath = archiveDirectory.getParent().relativize(path).toString();
      properties.put(ECLIPSELINK_PERSISTENCE_XML, descriptorPath);
      return properties;
    }

    @Override
    public EntityManagerFactory createEntityManagerFactory(@Nonnull RealmContext realmContext)
        throws IOException {
      var properties = transformJdbcUrl(loadProperties(), realmContext);
      Path archiveDirectory = path.getParent();
      ClassLoader prevClassLoader = Thread.currentThread().getContextClassLoader();
      try (URLClassLoader currentClassLoader =
          new URLClassLoader(
              new URL[] {archiveDirectory.getParent().toUri().toURL()},
              this.getClass().getClassLoader())) {
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        return Persistence.createEntityManagerFactory(persistenceUnitName, properties);
      } finally {
        Thread.currentThread().setContextClassLoader(prevClassLoader);
      }
    }
  }

  record JarFilePolarisEclipseLinkPersistenceUnit(
      URL confUrl, URL jarUrl, String descriptorPath, String persistenceUnitName)
      implements PolarisEclipseLinkPersistenceUnit {

    @Override
    public Map<String, String> loadProperties() throws IOException {
      var properties = internalLoadProperties(confUrl, persistenceUnitName);
      properties.put(ECLIPSELINK_PERSISTENCE_XML, descriptorPath);
      return properties;
    }

    @Override
    public EntityManagerFactory createEntityManagerFactory(@Nonnull RealmContext realmContext)
        throws IOException {
      var properties = transformJdbcUrl(loadProperties(), realmContext);
      ClassLoader prevClassLoader = Thread.currentThread().getContextClassLoader();
      try (URLClassLoader currentClassLoader =
          new URLClassLoader(new URL[] {jarUrl}, this.getClass().getClassLoader())) {
        Thread.currentThread().setContextClassLoader(currentClassLoader);
        return Persistence.createEntityManagerFactory(persistenceUnitName, properties);
      } finally {
        Thread.currentThread().setContextClassLoader(prevClassLoader);
      }
    }
  }

  static PolarisEclipseLinkPersistenceUnit locatePersistenceUnit(
      @Nullable String confFile, @Nullable String persistenceUnitName) throws IOException {
    if (persistenceUnitName == null) {
      persistenceUnitName = "polaris";
    }
    if (confFile == null) {
      confFile = "META-INF/persistence.xml";
    }
    // Try an embedded config file first
    int splitPosition = confFile.indexOf("!/");
    if (splitPosition != -1) {
      String jarPrefix = confFile.substring(0, splitPosition);
      String descriptorPath = confFile.substring(splitPosition + 2);
      URL jarUrl = classpathResource(jarPrefix);
      if (jarUrl != null) {
        // The JAR is in the classpath
        URL confUrl = URI.create("jar:" + jarUrl + "!/" + descriptorPath).toURL();
        return new ClasspathResourcePolarisEclipseLinkPersistenceUnit(
            confUrl, confFile, persistenceUnitName);
      } else {
        // The JAR is in the filesystem
        jarUrl = fileSystemPath(jarPrefix).toUri().toURL();
        URL confUrl = URI.create("jar:" + jarUrl + "!/" + descriptorPath).toURL();
        return new JarFilePolarisEclipseLinkPersistenceUnit(
            confUrl, jarUrl, descriptorPath, persistenceUnitName);
      }
    }
    // Try a classpath resource next
    URL resource = classpathResource(confFile);
    if (resource != null) {
      return new ClasspathResourcePolarisEclipseLinkPersistenceUnit(
          resource, confFile, persistenceUnitName);
    }
    // Try a filesystem path last
    try {
      return new FileSystemPolarisEclipseLinkPersistenceUnit(
          fileSystemPath(confFile), persistenceUnitName);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot find classpath resource or file: " + confFile, e);
    }
  }

  private static Path fileSystemPath(String pathStr) {
    Path path = Paths.get(pathStr);
    if (!Files.exists(path) || !Files.isRegularFile(path)) {
      throw new IllegalStateException("Not a regular file: " + pathStr);
    }
    return path.normalize().toAbsolutePath();
  }

  @Nullable
  private static URL classpathResource(String resourceName) throws IOException {
    Enumeration<URL> resources =
        Thread.currentThread().getContextClassLoader().getResources(resourceName);
    if (resources.hasMoreElements()) {
      URL resource = resources.nextElement();
      if (resources.hasMoreElements()) {
        throw new IllegalStateException(
            "Multiple resources found in classpath for " + resourceName);
      }
      return resource;
    }
    return null;
  }

  /** Load the persistence unit properties from a given configuration file */
  private static Map<String, String> internalLoadProperties(
      @Nonnull URL confFile, @Nonnull String persistenceUnitName) throws IOException {
    try (InputStream input = confFile.openStream()) {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.parse(input);
      XPath xPath = XPathFactory.newInstance().newXPath();
      String expression =
          "/persistence/persistence-unit[@name='" + persistenceUnitName + "']/properties/property";
      NodeList nodeList =
          (NodeList) xPath.compile(expression).evaluate(doc, XPathConstants.NODESET);
      Map<String, String> properties = new HashMap<>();
      for (int i = 0; i < nodeList.getLength(); i++) {
        NamedNodeMap nodeMap = nodeList.item(i).getAttributes();
        properties.put(
            nodeMap.getNamedItem("name").getNodeValue(),
            nodeMap.getNamedItem("value").getNodeValue());
      }

      return properties;
    } catch (XPathExpressionException
        | ParserConfigurationException
        | SAXException
        | IOException e) {
      String str =
          String.format(
              "Cannot find or parse the configuration file %s for persistence-unit %s",
              confFile, persistenceUnitName);
      throw new IOException(str, e);
    }
  }

  private static Map<String, String> transformJdbcUrl(
      Map<String, String> properties, RealmContext realmContext) {
    if (properties.containsKey(JDBC_URL)) {
      properties.put(
          JDBC_URL, properties.get(JDBC_URL).replace("{realm}", realmContext.getRealmIdentifier()));
    }
    return properties;
  }
}
