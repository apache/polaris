package io.polaris.core.storage;

import io.polaris.core.context.CallContext;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

/**
 * Base class for in-memory implementations of {@link PolarisStorageIntegration}. A basic
 * implementation of {@link #validateAccessToLocations(PolarisStorageConfigurationInfo, Set, Set)}
 * is provided that checks to see that the list of locations being accessed is among the list of
 * {@link PolarisStorageConfigurationInfo#allowedLocations}. Locations being accessed must be equal
 * to or a subdirectory of at least one of the allowed locations.
 *
 * @param <T>
 */
public abstract class InMemoryStorageIntegration<T extends PolarisStorageConfigurationInfo>
    extends PolarisStorageIntegration<T> {

  public InMemoryStorageIntegration(String identifierOrId) {
    super(identifierOrId);
  }

  /**
   * Check that the locations being accessed are all equal to or subdirectories of at least one of
   * the {@link PolarisStorageConfigurationInfo#allowedLocations}.
   *
   * @param storageConfig
   * @param actions a set of operation actions to validate, like LIST/READ/DELETE/WRITE/ALL
   * @param locations a set of locations to get access to
   * @return a map of location to a validation result for each action passed in. In this
   *     implementation, all actions have the same validation result, as we only verify the
   *     locations are equal to or subdirectories of the allowed locations.
   */
  public static Map<String, Map<PolarisStorageActions, ValidationResult>>
      validateSubpathsOfAllowedLocations(
          @NotNull PolarisStorageConfigurationInfo storageConfig,
          @NotNull Set<PolarisStorageActions> actions,
          @NotNull Set<String> locations) {
    // trim trailing / from allowed locations so that locations missing the trailing slash still
    // match
    // TODO: Canonicalize with URI and compare scheme/authority/path components separately
    TreeSet<String> allowedLocations =
        storageConfig.getAllowedLocations().stream()
            .map(
                str -> {
                  if (str.endsWith("/") && str.length() > 1) {
                    return str.substring(0, str.length() - 1);
                  } else {
                    return str;
                  }
                })
            .map(str -> str.replace("file:///", "file:/"))
            .collect(Collectors.toCollection(TreeSet::new));
    boolean allowWildcardLocation =
        Optional.ofNullable(CallContext.getCurrentContext())
            .flatMap(c -> Optional.ofNullable(c.getPolarisCallContext()))
            .map(
                pc ->
                    pc.getConfigurationStore()
                        .getConfiguration(pc, "ALLOW_WILDCARD_LOCATION", false))
            .orElse(false);

    if (allowWildcardLocation && allowedLocations.contains("*")) {
      return locations.stream()
          .collect(
              Collectors.toMap(
                  Function.identity(),
                  loc ->
                      actions.stream()
                          .collect(
                              Collectors.toMap(
                                  Function.identity(),
                                  a ->
                                      new ValidationResult(
                                          true, loc + " in the list of allowed locations")))));
    }
    Map<String, Map<PolarisStorageActions, ValidationResult>> resultMap = new HashMap<>();
    for (String rawLocation : locations) {
      String location = rawLocation.replace("file:///", "file:/");
      StringBuilder builder = new StringBuilder();
      NavigableSet<String> prefixes = allowedLocations;
      boolean validLocation = false;
      for (char c : location.toCharArray()) {
        builder.append(c);
        prefixes = allowedLocations.tailSet(builder.toString(), true);
        if (prefixes.isEmpty()) {
          break;
        } else if (prefixes.first().equals(builder.toString())) {
          validLocation = true;
          break;
        }
      }
      final boolean isValidLocation = validLocation;
      Map<PolarisStorageActions, ValidationResult> locationResult =
          actions.stream()
              .collect(
                  Collectors.toMap(
                      Function.identity(),
                      a ->
                          new ValidationResult(
                              isValidLocation,
                              rawLocation
                                  + " is "
                                  + (isValidLocation ? "" : "not ")
                                  + "in the list of allowed locations: "
                                  + allowedLocations)));

      resultMap.put(rawLocation, locationResult);
    }
    return resultMap;
  }

  @Override
  @NotNull
  public Map<String, Map<PolarisStorageActions, ValidationResult>> validateAccessToLocations(
      @NotNull T storageConfig,
      @NotNull Set<PolarisStorageActions> actions,
      @NotNull Set<String> locations) {
    return validateSubpathsOfAllowedLocations(storageConfig, actions, locations);
  }
}
