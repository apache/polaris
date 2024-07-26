package io.polaris.service.storage.azure;

import io.polaris.core.storage.azure.AzureLocation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AzureLocationTest {

  @Test
  public void testLocation() {
    String uri = "abfss://container@storageaccount.blob.core.windows.net/myfile";
    AzureLocation azureLocation = new AzureLocation(uri);
    Assertions.assertEquals("container", azureLocation.getContainer());
    Assertions.assertEquals("storageaccount", azureLocation.getStorageAccount());
    Assertions.assertEquals("blob.core.windows.net", azureLocation.getEndpoint());
    Assertions.assertEquals("myfile", azureLocation.getFilePath());
  }

  @Test
  public void testLocation_negative_cases() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new AzureLocation("wasbs://container@storageaccount.blob.core.windows.net/myfile"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new AzureLocation("abfss://storageaccount.blob.core.windows.net/myfile"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new AzureLocation("abfss://container@storageaccount/myfile"));
  }
}
