package org.apache.polaris.containerspec;

import java.util.Locale;
import org.testcontainers.utility.Base58;

public final class TestcontainerNames {

  private TestcontainerNames() {}

  public static String randomPrefixed(String prefix) {
    return prefix + "-" + Base58.randomString(6).toLowerCase(Locale.ROOT);
  }
}
