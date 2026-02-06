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
package org.apache.polaris.test.objectstoragemock.s3;

public final class S3Constants {

  public static final String RANGE = "Range";
  public static final String X_AMZ_REQUEST_ID = "x-amz-request-id";

  private S3Constants() {}

  public static final String CONTINUATION_TOKEN = "continuation-token";
  public static final String ENCODING_TYPE = "encoding-type";
  public static final String LIST_TYPE = "list-type";
  public static final String MAX_KEYS = "max-keys";
  public static final String START_AFTER = "start-after";
  public static final String TAGGING = "tagging";
  public static final String UPLOADS = "uploads";

  public static final String UPLOAD_ID = "uploadId";
}
