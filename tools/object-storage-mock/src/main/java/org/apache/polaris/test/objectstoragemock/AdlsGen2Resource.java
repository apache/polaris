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

import static com.google.common.net.HttpHeaders.CONTENT_RANGE;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_LENGTH;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static org.apache.polaris.test.objectstoragemock.adlsgen2.DataLakeStorageError.dataLakeStorageErrorObj;
import static org.apache.polaris.test.objectstoragemock.s3.S3Constants.RANGE;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.polaris.test.objectstoragemock.adlsgen2.ImmutablePath;
import org.apache.polaris.test.objectstoragemock.adlsgen2.ImmutablePathList;
import org.apache.polaris.test.objectstoragemock.adlsgen2.UpdateAction;
import org.apache.polaris.test.objectstoragemock.util.Holder;
import org.apache.polaris.test.objectstoragemock.util.PrefixSpliterator;
import org.apache.polaris.test.objectstoragemock.util.StartAfterSpliterator;

@Path("/adlsgen2/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AdlsGen2Resource {
  @Inject ObjectStorageMock mockServer;

  static final String delimiter = "/";

  // See
  // https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create?view=rest-storageservices-datalakestoragegen2-2019-12-12
  @PUT
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}/{path:.*}")
  @Consumes(MediaType.WILDCARD)
  // DataLakeFileClient.uploadWithResponse(...) sends "Accept: application/json"
  // DataLakeFileClient.getOutputStream(...) sends "Accept: application/xml"
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
  public Response create(
      @PathParam("filesystem") String filesystem,
      @PathParam("path") String path,
      @HeaderParam("x-ms-blob-type") String msBlobType,
      @HeaderParam("x-ms-blob-content-type") String msBlobContentType,
      @HeaderParam(ACCEPT) String accept,
      InputStream input) {

    String normalizedPath = stripLeadingSlash(path);

    return withFilesystem(
        filesystem,
        normalizedPath,
        b -> {
          Bucket.ObjectUpdater updater =
              b.updater().update(normalizedPath, Bucket.UpdaterMode.CREATE_NEW);

          if ("BlockBlob".equals(msBlobType) && MediaType.APPLICATION_XML.equals(accept)) {
            // Blob service - DataLakeFileClient.getOutputStream(...)
            updater.append(0L, input);
            if (msBlobContentType != null) {
              updater.setContentType(msBlobContentType);
            }
          }

          updater.commit();
          return Response.status(Status.CREATED).build();
        });
  }

  @PATCH
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}/{path:.*}")
  @Consumes(MediaType.WILDCARD)
  public Response update(
      @PathParam("filesystem") String filesystem,
      @PathParam("path") String path,
      @QueryParam("action") UpdateAction action,
      @QueryParam("flush") @DefaultValue("false") boolean flush,
      @HeaderParam("x-ms-content-type") String msContentType,
      InputStream input) {

    String normalizedPath = stripLeadingSlash(path);

    return withFilesystem(
        filesystem,
        normalizedPath,
        b -> {
          if (!action.appendOrFlush()) {
            return notImplemented();
          }
          Bucket.ObjectUpdater updater =
              b.updater().update(normalizedPath, Bucket.UpdaterMode.UPDATE);
          if (updater == null) {
            return keyNotFound();
          }
          if (action == UpdateAction.append) {
            updater.append(0L, input);
          }
          boolean doFlush = action == UpdateAction.flush || flush;
          if (doFlush) {
            updater
                .flush()
                .setContentType(msContentType != null ? msContentType : "application/octet-stream");
          }
          updater.commit();
          return Response.status(doFlush ? Status.OK : Status.ACCEPTED).build();
        });
  }

  @GET
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}/{path:.*}")
  @Produces(MediaType.WILDCARD)
  public Response read(
      @PathParam("filesystem") String filesystem,
      @PathParam("path") String path,
      @HeaderParam(RANGE) Range range) {

    String normalizedPath = stripLeadingSlash(path);

    return withFilesystem(
        filesystem,
        normalizedPath,
        b -> {
          MockObject obj = b.object().retrieve(normalizedPath);
          if (obj == null) {
            return keyNotFound();
          }

          String contentType;
          switch (obj.contentType()) {
            case "text/plain":
            case "application/json":
              contentType = obj.contentType();
              break;
            default:
              contentType = "application/octet-stream";
              break;
          }

          StreamingOutput stream = output -> obj.writer().write(range, output);

          long start = range != null ? range.start() : 0L;
          long end =
              range != null ? Math.min(range.end(), obj.contentLength()) : obj.contentLength();

          return Response.ok(stream)
              .tag(obj.etag())
              .type(contentType)
              .header(CONTENT_LENGTH, obj.contentLength())
              .header(CONTENT_RANGE, "bytes " + start + "-" + end + "/" + obj.contentLength())
              .lastModified(new Date(obj.lastModified()))
              .build();
        });
  }

  @HEAD
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}/{path:.*}")
  @Produces(MediaType.WILDCARD)
  public Response getProperties(
      @PathParam("filesystem") String filesystem,
      @PathParam("path") String path,
      @HeaderParam(RANGE) Range range) {

    String normalizedPath = stripLeadingSlash(path);

    return withFilesystem(
        filesystem,
        normalizedPath,
        b -> {
          MockObject obj = b.object().retrieve(normalizedPath);
          if (obj == null) {
            return keyNotFound();
          }

          Response.ResponseBuilder responseBuilder =
              Response.ok()
                  .tag(obj.etag())
                  .type(obj.contentType())
                  .lastModified(new Date(obj.lastModified()));
          if (range == null) {
            responseBuilder.header(CONTENT_LENGTH, obj.contentLength());
          }
          return responseBuilder.build();
        });
  }

  // See
  // https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list?view=rest-storageservices-datalakestoragegen2-2019-12-12
  @DELETE
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}/{path:.*}")
  @Consumes(MediaType.WILDCARD)
  public Response delete(
      @PathParam("filesystem") String filesystem,
      @PathParam("path") String path,
      @QueryParam("continuation") String continuationToken,
      @QueryParam("recursive") @DefaultValue("false") boolean recursive) {
    // No clue why there are pagination parameters, although there's no response

    String normalizedPath = stripLeadingSlash(path);

    return withFilesystem(
        filesystem,
        normalizedPath,
        b -> {
          if (recursive) {
            try (Stream<Bucket.ListElement> listStream =
                b.lister().list(normalizedPath, continuationToken)) {
              splitForDirectory(normalizedPath, continuationToken, listStream)
                  .forEachRemaining(e -> b.deleter().delete(e.key()));
            }
          } else {
            MockObject o = b.object().retrieve(normalizedPath);
            if (o == null) {
              return keyNotFound();
            }

            if (!b.deleter().delete(normalizedPath)) {
              return keyNotFound();
            }
          }
          return Response.ok().build();
        });
  }

  // See
  // https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list?view=rest-storageservices-datalakestoragegen2-2019-12-12
  @GET
  @Path("/{filesystem:[$a-z0-9](?!.*--)[-a-z0-9]{1,61}[a-z0-9]}")
  public Response list(
      @PathParam("filesystem") String filesystem,
      @QueryParam("directory") String directory,
      @QueryParam("continuation") String continuationToken,
      @QueryParam("maxResults") Integer maxResults) {

    // TODO handle 'recursive' - it's special, like everything from MS

    String normalizedPath = stripLeadingSlash(directory);

    return withFilesystem(
        filesystem,
        normalizedPath,
        b -> {
          try (Stream<Bucket.ListElement> listStream =
              b.lister().list(normalizedPath, continuationToken)) {
            ImmutablePathList.Builder result = ImmutablePathList.builder();

            int maxKeys = maxResults != null ? maxResults : Integer.MAX_VALUE;

            String nextContinuationToken = null;
            int keyCount = 0;
            String lastKey = null;

            Spliterator<Bucket.ListElement> split =
                splitForDirectory(normalizedPath, continuationToken, listStream);

            Holder<Bucket.ListElement> current = new Holder<>();
            while (split.tryAdvance(current::set)) {
              if (keyCount == maxKeys) {
                nextContinuationToken = lastKey;
                break;
              }

              String key = current.get().key();

              MockObject obj = current.get().object();
              result.addPaths(
                  ImmutablePath.builder()
                      .name(key)
                      .etag(obj.etag())
                      .contentLength(obj.contentLength())
                      .lastModified(
                          RFC_1123_DATE_TIME.format(
                              ZonedDateTime.ofInstant(
                                  Instant.ofEpochMilli(obj.lastModified()), ZoneId.of("UTC"))))
                      .creationTime(1000L) // cannot be zero
                      .directory(false)
                      .build());
              keyCount++;
              lastKey = key;
            }

            Response.ResponseBuilder response = Response.ok(result.build());
            if (nextContinuationToken != null) {
              response.header("x-ms-continuation", nextContinuationToken);
            }
            return response.build();
          }
        });
  }

  private String stripLeadingSlash(String path) {
    if (path == null) {
      return "";
    }
    return path.startsWith("/") ? path.substring(1) : path;
  }

  private static Spliterator<Bucket.ListElement> splitForDirectory(
      String directory, String offset, Stream<Bucket.ListElement> listStream) {

    Spliterator<Bucket.ListElement> split = listStream.spliterator();

    if (offset != null) {
      split = new StartAfterSpliterator<>(split, e -> e.key().compareTo(offset) >= 0);
    }

    if (directory == null || directory.isEmpty()) {
      return split;
    }

    if (!directory.endsWith(delimiter)) {
      directory += delimiter;
    }

    String directoryPrefix = directory;
    return new PrefixSpliterator<>(split, e -> e.key().startsWith(directoryPrefix));
  }

  private static Response bucketNotFound() {
    return dataLakeStorageError(
        Status.NOT_FOUND, "FilesystemNotFound", "The specified filesystem does not exist.");
  }

  private static Response keyNotFound() {
    return dataLakeStorageError(
        Status.NOT_FOUND, "PathNotFound", "The specified path does not exist.");
  }

  private static Response accessDenied() {
    return dataLakeStorageError(Status.FORBIDDEN, "Forbidden", "Access Denied.");
  }

  private static Response dataLakeStorageError(Status status, String code, String message) {
    return Response.status(status)
        .header("x-ms-error-code", code)
        .type(MediaType.APPLICATION_JSON)
        .entity(dataLakeStorageErrorObj(code, message).error())
        .build();
  }

  private static Response notImplemented() {
    return Response.status(Status.NOT_IMPLEMENTED).build();
  }

  private Response withFilesystem(
      String filesystem, String path, Function<Bucket, Response> worker) {
    if (!mockServer.accessCheckHandler().accessAllowed(path)) {
      return accessDenied();
    }

    Bucket bucket = mockServer.buckets().get(filesystem);
    if (bucket == null) {
      return bucketNotFound();
    }
    return worker.apply(bucket);
  }
}
