package io.polaris.service.admin;

import static io.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.polaris.core.admin.model.AwsStorageConfigInfo;
import io.polaris.core.admin.model.Catalog;
import io.polaris.core.admin.model.CatalogProperties;
import io.polaris.core.admin.model.CreateCatalogRequest;
import io.polaris.core.admin.model.StorageConfigInfo;
import io.polaris.service.PolarisApplication;
import io.polaris.service.config.PolarisApplicationConfig;
import io.polaris.service.test.PolarisConnectionExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({DropwizardExtensionsSupport.class, PolarisConnectionExtension.class})
public class PolarisOverlappingCatalogTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          // Bind to random port to support parallelism
          ConfigOverride.config("server.applicationConnectors[0].port", "0"),
          ConfigOverride.config("server.adminConnectors[0].port", "0"),
          // Block overlapping catalog paths:
          ConfigOverride.config("featureConfiguration.ALLOW_OVERLAPPING_CATALOG_URLS", "false"));
  private static String userToken;
  private static String realm;

  @BeforeAll
  public static void setup(PolarisConnectionExtension.PolarisToken adminToken) {
    userToken = adminToken.token();
    realm = PolarisConnectionExtension.getTestRealm(PolarisServiceImplIntegrationTest.class);
  }

  private Response createCatalog(String prefix, String defaultBaseLocation, boolean isExternal) {
    return createCatalog(prefix, defaultBaseLocation, isExternal, new ArrayList<String>());
  }

  private static Invocation.Builder request() {
    return EXT.client()
        .target(String.format("http://localhost:%d/api/management/v1/catalogs", EXT.getLocalPort()))
        .request("application/json")
        .header("Authorization", "Bearer " + userToken)
        .header(REALM_PROPERTY_KEY, realm);
  }

  private Response createCatalog(
      String prefix,
      String defaultBaseLocation,
      boolean isExternal,
      List<String> allowedLocations) {
    String uuid = UUID.randomUUID().toString();
    StorageConfigInfo config =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::123456789012:role/my-role")
            .setExternalId("externalId")
            .setUserArn("userArn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(
                allowedLocations.stream()
                    .map(
                        l -> {
                          return String.format("s3://bucket/%s/%s", prefix, l);
                        })
                    .toList())
            .build();
    Catalog catalog =
        new Catalog(
            isExternal ? Catalog.TypeEnum.EXTERNAL : Catalog.TypeEnum.INTERNAL,
            String.format("overlap_catalog_%s", uuid),
            new CatalogProperties(String.format("s3://bucket/%s/%s", prefix, defaultBaseLocation)),
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            1,
            config);
    try (Response response = request().post(Entity.json(new CreateCatalogRequest(catalog)))) {
      return response;
    }
  }

  @Test
  public void testBasicOverlappingCatalogs() {
    Arrays.asList(false, true)
        .forEach(
            initiallyExternal -> {
              Arrays.asList(false, true)
                  .forEach(
                      laterExternal -> {
                        String prefix = UUID.randomUUID().toString();

                        assertThat(createCatalog(prefix, "root", initiallyExternal))
                            .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

                        // OK, non-overlapping
                        assertThat(createCatalog(prefix, "boot", laterExternal))
                            .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

                        // OK, non-overlapping due to no `/`
                        assertThat(createCatalog(prefix, "roo", laterExternal))
                            .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

                        // Also OK due to no `/`
                        assertThat(createCatalog(prefix, "root.child", laterExternal))
                            .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

                        // inside `root`
                        assertThat(createCatalog(prefix, "root/child", laterExternal))
                            .returns(
                                Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

                        // `root` is inside this
                        assertThat(createCatalog(prefix, "", laterExternal))
                            .returns(
                                Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
                      });
            });
  }

  @Test
  public void testAllowedLocationOverlappingCatalogs() {
    Arrays.asList(false, true)
        .forEach(
            initiallyExternal -> {
              Arrays.asList(false, true)
                  .forEach(
                      laterExternal -> {
                        String prefix = UUID.randomUUID().toString();

                        assertThat(
                                createCatalog(
                                    prefix,
                                    "animals",
                                    initiallyExternal,
                                    Arrays.asList("dogs", "cats")))
                            .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

                        // OK, non-overlapping
                        assertThat(
                                createCatalog(
                                    prefix,
                                    "danimals",
                                    laterExternal,
                                    Arrays.asList("dan", "daniel")))
                            .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);

                        // This DBL overlaps with initial AL
                        assertThat(
                                createCatalog(
                                    prefix,
                                    "dogs",
                                    initiallyExternal,
                                    Arrays.asList("huskies", "labs")))
                            .returns(
                                Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

                        // This AL overlaps with initial DBL
                        assertThat(
                                createCatalog(
                                    prefix,
                                    "kingdoms",
                                    initiallyExternal,
                                    Arrays.asList("plants", "animals")))
                            .returns(
                                Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);

                        // This AL overlaps with an initial AL
                        assertThat(
                                createCatalog(
                                    prefix,
                                    "plays",
                                    initiallyExternal,
                                    Arrays.asList("rent", "cats")))
                            .returns(
                                Response.Status.BAD_REQUEST.getStatusCode(), Response::getStatus);
                      });
            });
  }
}
