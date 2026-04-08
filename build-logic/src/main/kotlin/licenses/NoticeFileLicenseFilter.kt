/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package licenses

import com.github.jk1.license.ProjectData
import com.github.jk1.license.filter.DependencyFilter

/**
 * Removes license detections originating from NOTICE files. NOTICE files document third-party
 * attributions and may mention licenses of bundled components; these should not be treated as the
 * artifact's own license.
 */
class NoticeFileLicenseFilter : DependencyFilter {
  override fun filter(data: ProjectData?): ProjectData {
    data!!
    data.allDependencies.forEach { mod ->
      mod.licenseFiles.forEach { licenseFileData ->
        licenseFileData.fileDetails.removeAll { detail ->
          detail.file != null && detail.file.lowercase().contains("/notice")
        }
      }
    }
    return data
  }
}
