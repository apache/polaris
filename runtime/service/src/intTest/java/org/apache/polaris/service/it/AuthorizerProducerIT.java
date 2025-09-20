package org.apache.polaris.service.it;

import jakarta.inject.Inject;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.OpaPolarisAuthorizer;
import org.junit.jupiter.api.Test;
import io.quarkus.test.junit.QuarkusTest;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class AuthorizerProducerIT {

    @Inject
    PolarisAuthorizer polarisAuthorizer;

    @Test
    void testDefaultAuthorizerIsImpl() {
        // Should be PolarisAuthorizerImpl if polaris.authorization.implementation=default
        assertTrue(polarisAuthorizer instanceof PolarisAuthorizerImpl,
            "Default authorizer should be PolarisAuthorizerImpl");
    }

    // You can add more tests to check OPA wiring by overriding config in test resources
}
