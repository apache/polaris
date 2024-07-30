package io.polaris.core.storage;

import org.jetbrains.annotations.NotNull;

public class StorageUtil {
  /**
   * Concatenating two file paths by making sure one and only one path separator is placed between
   * the two paths.
   *
   * @param leftPath left path
   * @param rightPath right path
   * @param fileSep File separator to use.
   * @return Well formatted file path.
   */
  public static @NotNull String concatFilePrefixes(
      @NotNull String leftPath, String rightPath, String fileSep) {
    if (leftPath.endsWith(fileSep) && rightPath.startsWith(fileSep)) {
      return leftPath + rightPath.substring(1);
    } else if (!leftPath.endsWith(fileSep) && !rightPath.startsWith(fileSep)) {
      return leftPath + fileSep + rightPath;
    } else {
      return leftPath + rightPath;
    }
  }
}
