package io.polaris.core.storage;

public enum PolarisStorageActions {
  READ,
  WRITE,
  LIST,
  DELETE,
  ALL,
  ;

  /** check if the provided string is a valid action. */
  public static boolean isValidAction(String s) {
    for (PolarisStorageActions action : PolarisStorageActions.values()) {
      if (action.name().equalsIgnoreCase(s)) {
        return true;
      }
    }
    return false;
  }
}
