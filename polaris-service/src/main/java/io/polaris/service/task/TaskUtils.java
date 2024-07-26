package io.polaris.service.task;

import java.io.IOException;
import org.apache.commons.codec.binary.Base64;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;

public class TaskUtils {
  static boolean exists(String path, FileIO fileIO) {
    try {
      return fileIO.newInputFile(path).exists();
    } catch (NotFoundException e) {
      // in-memory FileIO throws this exception
      return false;
    } catch (Exception e) {
      // typically, clients will catch a 404 and simply return false, so any other exception
      // means something probably went wrong
      throw new RuntimeException(e);
    }
  }

  /**
   * base64 encode the serialized manifest file entry so we can deserialize it and read the manifest
   * in the {@link ManifestFileCleanupTaskHandler}
   *
   * @param mf
   * @return
   */
  static String encodeManifestFile(ManifestFile mf) {
    try {
      return Base64.encodeBase64String(ManifestFiles.encode(mf));
    } catch (IOException e) {
      throw new RuntimeException("Unable to encode binary data in memory", e);
    }
  }
}
