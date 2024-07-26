package io.polaris.service.task;

import io.polaris.core.context.CallContext;
import io.polaris.core.entity.PolarisTaskConstants;
import io.polaris.core.entity.TaskEntity;
import io.polaris.core.persistence.MetaStoreManagerFactory;
import io.polaris.core.persistence.PolarisMetaStoreManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;

public class TaskFileIOSupplier implements Function<TaskEntity, FileIO> {
  private final MetaStoreManagerFactory metaStoreManagerFactory;

  public TaskFileIOSupplier(MetaStoreManagerFactory metaStoreManagerFactory) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  @Override
  public FileIO apply(TaskEntity task) {
    Map<String, String> internalProperties = task.getInternalPropertiesAsMap();
    String location = internalProperties.get(PolarisTaskConstants.STORAGE_LOCATION);
    PolarisMetaStoreManager metaStoreManager =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(
            CallContext.getCurrentContext().getRealmContext());
    Map<String, String> properties = new HashMap<>(internalProperties);
    properties.putAll(
        metaStoreManagerFactory
            .getOrCreateStorageCredentialCache(CallContext.getCurrentContext().getRealmContext())
            .getOrGenerateSubScopeCreds(
                metaStoreManager,
                CallContext.getCurrentContext().getPolarisCallContext(),
                task,
                true,
                Set.of(location),
                Set.of(location)));
    String ioImpl =
        properties.getOrDefault(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
    return CatalogUtil.loadFileIO(ioImpl, properties, new Configuration());
  }
}
