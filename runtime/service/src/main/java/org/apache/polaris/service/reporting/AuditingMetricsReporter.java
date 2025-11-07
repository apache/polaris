package org.apache.polaris.service.reporting;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Date;
import net.minidev.json.JSONObject;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;

@RequestScoped
@Identifier("audit")
public class AuditingMetricsReporter implements PolarisMetricsReporter {
  private final SecurityContext securityContext;

  @Inject
  public AuditingMetricsReporter(SecurityContext securityContext) {
    this.securityContext = securityContext;
  }

  private String getUserName() {
    if (securityContext.getUserPrincipal() != null) {
      return securityContext.getUserPrincipal().getName();
    } else {
      throw new NotAuthorizedException("Could not get user from security context");
    }
  }

  private JSONObject startAuditObject(String user) {
    JSONObject output = new JSONObject();
    output.put("type", "audit"); // REQUIRED FOR AUDIT LOGS
    output.put("timestamp", new Date().toString());
    output.put("user", user);
    return output;
  }

  @Override
  public void reportMetric(String prefix, TableIdentifier table, MetricsReport report) {
    JSONObject output = startAuditObject(getUserName());
    if (report instanceof CommitReport commitReport) {
      output.put("record", "commit");
      output.put("table", commitReport.tableName());
      output.put("tableSnapshot", commitReport.snapshotId());
      output.put("operation", commitReport.operation());
      CommitMetricsResult metrics = commitReport.commitMetrics();
      output.put("elapsedTime", metrics.totalDuration().totalDuration().toMillis());
      if (metrics.addedDataFiles() != null) {
        output.put("addedFiles", metrics.addedDataFiles().value());
      }
      if (metrics.removedDataFiles() != null) {
        output.put("removedFiles", metrics.removedDataFiles().value());
      }
    }
    if (report instanceof ScanReport scanReport) {
      output.put("record", "scan");
      output.put("table", scanReport.tableName());
      output.put("query", scanReport.filter().toString());
      output.put("fields", scanReport.projectedFieldNames());
      output.put(
          "elapsedTime",
          scanReport.scanMetrics().totalPlanningDuration().totalDuration().toMillis());
      output.put("tableSnapshot", scanReport.snapshotId());
    }
    System.out.println(output.toJSONString());
  }
}
