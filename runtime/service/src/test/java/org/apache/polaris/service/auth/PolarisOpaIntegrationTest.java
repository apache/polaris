package org.apache.polaris.service.auth;

import static io.restassured.RestAssured.given;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.polaris.test.commons.OpaIntegrationProfile;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(OpaIntegrationProfile.class)
public class PolarisOpaIntegrationTest {
  @Test
  void testOpaAllowsAdmin() {
    given()
        .header("X-Test-Principal", "admin")
        .when()
        .get("/api/catalog/namespaces")
        .then()
        .statusCode(200);
  }

  @Test
  void testOpaDeniesNonAdmin() {
    given()
        .header("X-Test-Principal", "bob")
        .when()
        .get("/api/catalog/namespaces")
        .then()
        .statusCode(403);
  }
}
