/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// CODE_COPIED_TO_POLARIS from Project Nessie 0.106.1
package org.apache.polaris.test.objectstoragemock;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.apache.polaris.test.objectstoragemock.sts.AssumeRoleResult;
import org.apache.polaris.test.objectstoragemock.sts.ImmutableAssumeRoleResponse;
import org.apache.polaris.test.objectstoragemock.sts.ImmutableResponseMetadata;

@Path("/sts/")
@Produces(MediaType.APPLICATION_XML)
@Consumes(MediaType.APPLICATION_XML)
public class StsResource {
  @Inject ObjectStorageMock mockServer;

  @Path("assumeRole")
  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.APPLICATION_XML)
  public Object assumeRole(
      @FormParam("Action") String action,
      @FormParam("Version") String version,
      @FormParam("RoleArn") String roleArn,
      @FormParam("RoleSessionName") String roleSessionName,
      @FormParam("Policy") String policy,
      @FormParam("DurationSeconds") Integer durationSeconds,
      @FormParam("ExternalId") String externalId,
      @FormParam("SerialNumber") String serialNumber,
      @HeaderParam("amz-sdk-invocation-id") String amzSdkInvocationId) {
    AssumeRoleResult result =
        mockServer
            .assumeRoleHandler()
            .assumeRole(
                action,
                version,
                roleArn,
                roleSessionName,
                policy,
                durationSeconds,
                externalId,
                serialNumber);
    return ImmutableAssumeRoleResponse.builder()
        .assumeRoleResult(result)
        .responseMetadata(ImmutableResponseMetadata.builder().requestId(amzSdkInvocationId).build())
        .build();
  }
}
