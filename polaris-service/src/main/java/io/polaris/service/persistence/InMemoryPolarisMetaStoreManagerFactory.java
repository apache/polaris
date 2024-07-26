package io.polaris.service.persistence;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.polaris.core.PolarisDiagnostics;
import io.polaris.core.context.RealmContext;
import io.polaris.core.persistence.LocalPolarisMetaStoreManagerFactory;
import io.polaris.core.persistence.PolarisMetaStoreManager;
import io.polaris.core.persistence.PolarisMetaStoreSession;
import io.polaris.core.persistence.PolarisTreeMapMetaStoreSessionImpl;
import io.polaris.core.persistence.PolarisTreeMapStore;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;

@JsonTypeName("in-memory")
public class InMemoryPolarisMetaStoreManagerFactory
    extends LocalPolarisMetaStoreManagerFactory<
        PolarisTreeMapStore, PolarisTreeMapMetaStoreSessionImpl> {
  Set<String> bootstrappedRealms = new HashSet<>();

  @Override
  protected PolarisTreeMapStore createBackingStore(@NotNull PolarisDiagnostics diagnostics) {
    return new PolarisTreeMapStore(diagnostics);
  }

  @Override
  protected PolarisMetaStoreSession createMetaStoreSession(
      @NotNull PolarisTreeMapStore store, @NotNull RealmContext realmContext) {
    return new PolarisTreeMapMetaStoreSessionImpl(store, storageIntegration);
  }

  @Override
  public synchronized PolarisMetaStoreManager getOrCreateMetaStoreManager(
      RealmContext realmContext) {
    String realmId = realmContext.getRealmIdentifier();
    if (!bootstrappedRealms.contains(realmId)) {
      bootstrapRealmAndPrintCredentials(realmId);
    }
    return super.getOrCreateMetaStoreManager(realmContext);
  }

  @Override
  public synchronized Supplier<PolarisMetaStoreSession> getOrCreateSessionSupplier(
      RealmContext realmContext) {
    String realmId = realmContext.getRealmIdentifier();
    if (!bootstrappedRealms.contains(realmId)) {
      bootstrapRealmAndPrintCredentials(realmId);
    }
    return super.getOrCreateSessionSupplier(realmContext);
  }

  private void bootstrapRealmAndPrintCredentials(String realmId) {
    Map<String, PolarisMetaStoreManager.PrincipalSecretsResult> results =
        this.bootstrapRealms(Arrays.asList(realmId));
    bootstrappedRealms.add(realmId);

    PolarisMetaStoreManager.PrincipalSecretsResult principalSecrets = results.get(realmId);

    String msg =
        String.format(
            "realm: %1s root principal credentials: %2s:%3s",
            realmId,
            principalSecrets.getPrincipalSecrets().getPrincipalClientId(),
            principalSecrets.getPrincipalSecrets().getMainSecret());
    System.out.println(msg);
  }
}
