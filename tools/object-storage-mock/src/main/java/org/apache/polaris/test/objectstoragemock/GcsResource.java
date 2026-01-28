/*
 * Copyright (C) 2022 Dremio
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

import static jakarta.ws.rs.core.HttpHeaders.CONTENT_LENGTH;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.HttpHeaders.IF_MATCH;
import static jakarta.ws.rs.core.HttpHeaders.IF_MODIFIED_SINCE;
import static jakarta.ws.rs.core.HttpHeaders.IF_NONE_MATCH;
import static jakarta.ws.rs.core.HttpHeaders.IF_UNMODIFIED_SINCE;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.polaris.test.objectstoragemock.s3.S3Constants.RANGE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.StreamingOutput;
import jakarta.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.polaris.test.objectstoragemock.Bucket.ListElement;
import org.apache.polaris.test.objectstoragemock.gcs.ImmutableErrorResponse;
import org.apache.polaris.test.objectstoragemock.gcs.ImmutableListResponse;
import org.apache.polaris.test.objectstoragemock.gcs.ImmutableStorageObject;
import org.apache.polaris.test.objectstoragemock.gcs.ObjectAlt;
import org.apache.polaris.test.objectstoragemock.gcs.StorageObject;
import org.apache.polaris.test.objectstoragemock.gcs.UploadType;
import org.apache.polaris.test.objectstoragemock.util.Holder;
import org.apache.polaris.test.objectstoragemock.util.PrefixSpliterator;
import org.apache.polaris.test.objectstoragemock.util.StartAfterSpliterator;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class GcsResource {
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  @Inject ObjectStorageMock mockServer;

  @GET
  @Path("/storage/v1/b")
  public Response listBuckets() {
    return notImplemented();
  }

  @PUT
  @Path("/storage/v1/b/{bucketName:[a-z0-9.-]+}")
  public Response getBucket() {
    return notImplemented();
  }

  @POST
  @Path("/storage/v1/b/{bucketName:[a-z0-9.-]+}")
  public Response createBucket() {
    return notImplemented();
  }

  @DELETE
  @Path("/storage/v1/b/{bucketName:[a-z0-9.-]+}")
  public Response deleteBucket() {
    return notImplemented();
  }

  @GET
  @Path("/storage/v1/b/{bucketName:[a-z0-9.-]+}/o")
  // TODO IF   params =  !UPLOADS
  public Response listObjects(
      @PathParam("bucketName") String bucketName,
      @QueryParam("delimiter") @DefaultValue("/") String delimiter,
      @QueryParam("endOffset") String endOffset,
      @QueryParam("maxResults") @DefaultValue("2147483647") int maxResults,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("prefix") String prefix,
      @QueryParam("startOffset") String startOffset) {
    return withBucket(
        bucketName,
        b -> {
          String offset = pageToken != null ? pageToken : startOffset;

          try (Stream<ListElement> listStream = b.lister().list(prefix, offset)) {

            String nextPageToken = null;
            int keyCount = 0;
            Set<String> prefixes = new HashSet<>();
            String lastKey = null;

            Spliterator<ListElement> split = listStream.spliterator();
            if (offset != null) {
              split = new StartAfterSpliterator<>(split, e -> e.key().compareTo(offset) >= 0);
            }

            if (prefix != null && !prefix.isEmpty()) {
              String effectivePrefix = prefix.endsWith(delimiter) ? prefix : (prefix + delimiter);
              split = new PrefixSpliterator<>(split, e -> e.key().startsWith(effectivePrefix));
            }

            ImmutableListResponse.Builder response = ImmutableListResponse.builder();

            Holder<ListElement> current = new Holder<>();
            while (split.tryAdvance(current::set)) {
              if (keyCount == maxResults) {
                nextPageToken = lastKey;
                break;
              }

              String key = current.get().key();

              if (endOffset != null && endOffset.compareTo(key) < 0) {
                break;
              }

              int i = key.lastIndexOf(delimiter);
              String pre = i > 0 ? key.substring(0, i) : "";

              MockObject obj = current.get().object();
              response.addItems(storageObject(bucketName, key, obj));
              keyCount++;
              lastKey = key;
              if (prefixes.add(pre)) {
                // response.addPrefixes(pre);
              }
            }

            response.nextPageToken(nextPageToken);

            return Response.ok(response.build()).build();
          }
        });
  }

  @DELETE
  @Path("/storage/v1/b/{bucketName:[a-z0-9.-]+}/o/{object:.+}")
  @Consumes(MediaType.WILDCARD) // Java GCS client sends application/x-www-form-urlencoded
  public Response deleteObject(
      @PathParam("bucketName") String bucketName, @PathParam("object") String objectName) {
    return withBucket(
        bucketName,
        objectName,
        b -> {
          if (!b.deleter().delete(objectName)) {
            return keyNotFound();
          }
          return noContent();
        });
  }

  @GET
  @Path("/download/storage/v1/b/{bucketName:[a-z0-9.-]+}/o/{object:.+}")
  @Produces(MediaType.WILDCARD)
  public Response downloadObject(
      @PathParam("bucketName") String bucketName,
      @PathParam("object") String objectName,
      @QueryParam("alt") @DefaultValue("json") ObjectAlt alt,
      @HeaderParam(RANGE) Range range,
      @HeaderParam(IF_MATCH) List<String> match,
      @HeaderParam(IF_NONE_MATCH) List<String> noneMatch,
      @HeaderParam(IF_MODIFIED_SINCE) Date modifiedSince,
      @HeaderParam(IF_UNMODIFIED_SINCE) Date unmodifiedSince) {
    return getObject(
        bucketName, objectName, alt, range, match, noneMatch, modifiedSince, unmodifiedSince);
  }

  @GET
  @Path("/storage/v1/b/{bucketName:[a-z0-9.-]+}/o/{object:.+}")
  @Produces(MediaType.WILDCARD)
  public Response getObject(
      @PathParam("bucketName") String bucketName,
      @PathParam("object") String objectName,
      @QueryParam("alt") @DefaultValue("json") ObjectAlt alt,
      @HeaderParam(RANGE) Range range,
      @HeaderParam(IF_MATCH) List<String> match,
      @HeaderParam(IF_NONE_MATCH) List<String> noneMatch,
      @HeaderParam(IF_MODIFIED_SINCE) Date modifiedSince,
      @HeaderParam(IF_UNMODIFIED_SINCE) Date unmodifiedSince) {
    if (range != null) {
      // TODO Iceberg does this :(    return notImplemented();
    }

    return withBucketObject(
        bucketName,
        objectName,
        obj -> {
          if (unmodifiedSince != null && unmodifiedSince.getTime() > obj.lastModified()) {
            return preconditionFailed();
          }
          if (modifiedSince != null && modifiedSince.getTime() > obj.lastModified()) {
            return notModified(obj.etag());
          }
          if (!match.isEmpty() && !match.contains(obj.etag())) {
            return preconditionFailed();
          }
          if (!noneMatch.isEmpty() && noneMatch.contains(obj.etag())) {
            return notModified(obj.etag());
          }

          switch (alt) {
            case json:
              return Response.ok(
                      storageObject(bucketName, objectName, obj), MediaType.APPLICATION_JSON_TYPE)
                  .build();
            case media:
              StreamingOutput stream = output -> obj.writer().write(range, output);
              Response.ResponseBuilder responseBuilder =
                  Response.ok(stream)
                      .tag(obj.etag())
                      .type(obj.contentType())
                      .lastModified(new Date(obj.lastModified()));
              if (range == null) {
                responseBuilder.header(CONTENT_LENGTH, obj.contentLength());
              }
              return responseBuilder.build();
            default:
              throw new IllegalArgumentException("alt = " + alt);
          }
        });
  }

  @PUT
  @Path("/upload/storage/v1/b/{bucketName:[a-z0-9.-]+}/o")
  @Consumes(MediaType.WILDCARD)
  // See https://cloud.google.com/storage/docs/performing-resumable-uploads#json-api
  public Response uploadStuff(
      @PathParam("bucketName") String bucketName,
      @QueryParam("name") String objectName,
      @QueryParam("uploadType") UploadType uploadType,
      @HeaderParam(CONTENT_TYPE) String contentType,
      @Context UriInfo uriInfo,
      InputStream stream) {
    return insertObject(bucketName, objectName, uploadType, contentType, uriInfo, stream);
  }

  @POST
  @Path("/upload/storage/v1/b/{bucketName:[a-z0-9.-]+}/o")
  @Consumes(MediaType.WILDCARD)
  // See https://cloud.google.com/storage/docs/uploading-objects#uploading-an-object
  public Response insertObject(
      @PathParam("bucketName") String bucketName,
      @QueryParam("name") String objectName,
      @QueryParam("uploadType") UploadType uploadType,
      @HeaderParam(CONTENT_TYPE) String contentType,
      @Context UriInfo uriInfo,
      InputStream stream) {
    return withBucket(
        bucketName,
        objectName,
        bucket -> {
          try {
            Bucket.Updater updater = bucket.updater();
            MockObject obj;
            switch (uploadType) {
              case media:
                // Not tested
                obj =
                    updater
                        .update(objectName, Bucket.UpdaterMode.UPSERT)
                        .append(0L, stream)
                        .setContentType(contentType)
                        .commit();
                return Response.ok(
                        storageObject(bucketName, objectName, obj), MediaType.APPLICATION_JSON_TYPE)
                    .build();
              case multipart:
                return notImplemented();
              case resumable:
                // read metadata as JSON - see
                // https://cloud.google.com/storage/docs/json_api/v1/objects/insert
                ObjectNode metadata = OBJECT_MAPPER.readValue(stream, ObjectNode.class);
                JsonNode contentTypeNode = metadata.get("contentType");
                String ct =
                    contentTypeNode != null
                        ? contentTypeNode.textValue()
                        : "application/octet-stream";
                obj =
                    updater
                        .update(objectName, Bucket.UpdaterMode.UPSERT)
                        .setContentType(ct)
                        .commit();

                return Response.ok(
                        storageObject(bucketName, objectName, obj), MediaType.APPLICATION_JSON_TYPE)
                    .header(
                        "Location",
                        uriInfo
                            .getBaseUri()
                            .resolve(
                                "upload/storage/v1/b/"
                                    + encode(bucketName, UTF_8)
                                    + "/"
                                    + "o?name="
                                    + encode(objectName, UTF_8)
                                    + "&uploadType="
                                    + UploadType.appendStuff.name()))
                    .build();
              case appendStuff:
                obj =
                    updater
                        .update(objectName, Bucket.UpdaterMode.UPDATE)
                        .append(0L, stream)
                        .commit();

                return Response.ok(
                        storageObject(bucketName, objectName, obj), MediaType.APPLICATION_JSON_TYPE)
                    .build();
              default:
                throw new IllegalArgumentException("Unknown upload type: " + uploadType);
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          } catch (UnsupportedOperationException e) {
            return Response.status(405, "POST object not allowed").build();
          }
        });
  }

  private static StorageObject storageObject(String bucketName, String key, MockObject obj) {
    return ImmutableStorageObject.builder()
        .etag(obj.etag())
        .name(key)
        .id(key)
        .size(obj.contentLength())
        .bucket(bucketName)
        .contentType(obj.contentType())
        .storageClass(obj.storageClass().name())
        .updated(Instant.ofEpochMilli(obj.lastModified()))
        .build();
  }

  private static Response preconditionFailed() {
    return errorResponse(Status.PRECONDITION_FAILED, "Precondition Failed");
  }

  private static Response notModified(String etag) {
    // Hint: HTTP/304 MUST NOT contain a message body (as per HTTP RFCs)
    return Response.notModified(etag).build();
  }

  private static Response noContent() {
    return Response.status(Status.NO_CONTENT).build();
  }

  private static Response bucketNotFound() {
    return errorResponse(Status.NOT_FOUND, "The specified bucket does not exist.");
  }

  private static Response keyNotFound() {
    return errorResponse(Status.NOT_FOUND, "The specified key does not exist.");
  }

  private static Response accessDenied() {
    return errorResponse(Status.FORBIDDEN, "Access Denied.");
  }

  private static Response errorResponse(Status status, String message) {
    return Response.status(status)
        .type(MediaType.APPLICATION_JSON)
        .entity(
            ImmutableErrorResponse.builder().code(status.getStatusCode()).message(message).build())
        .build();
  }

  private static Response notImplemented() {
    return Response.status(Status.NOT_IMPLEMENTED).build();
  }

  private Response withBucket(String bucketName, Function<Bucket, Response> worker) {
    Bucket bucket = mockServer.buckets().get(bucketName);
    if (bucket == null) {
      return bucketNotFound();
    }
    return worker.apply(bucket);
  }

  private Response withBucket(
      String bucketName, String objectName, Function<Bucket, Response> worker) {
    Bucket bucket = mockServer.buckets().get(bucketName);
    if (bucket == null) {
      return bucketNotFound();
    }

    if (!mockServer.accessCheckHandler().accessAllowed(objectName)) {
      return accessDenied();
    }

    return worker.apply(bucket);
  }

  private Response withBucketObject(
      String bucketName, String objectName, Function<MockObject, Response> worker) {
    return withBucket(
        bucketName,
        objectName,
        bucket -> {
          MockObject o = bucket.object().retrieve(objectName);
          if (o == null) {
            return keyNotFound();
          }
          return worker.apply(o);
        });
  }
}
