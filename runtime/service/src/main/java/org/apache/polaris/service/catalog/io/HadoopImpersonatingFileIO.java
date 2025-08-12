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
package org.apache.polaris.service.catalog.io;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * A delegating FileIO wrapper that executes all operations under a specific HDFS user context
 * using UserGroupInformation.doAs(). This enables per-catalog user impersonation for HDFS access.
 */
public class HadoopImpersonatingFileIO implements FileIO {

  private final FileIO delegate;
  private final UserGroupInformation ugi;

  public HadoopImpersonatingFileIO(FileIO delegate, String username) {
    this.delegate = delegate;
    this.ugi = UserGroupInformation.createRemoteUser(username);
  }

  @Override
  public InputFile newInputFile(String path) {
    try {
      return ugi.doAs((PrivilegedExceptionAction<InputFile>) () -> delegate.newInputFile(path));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create InputFile with user impersonation", e);
    }
  }

  @Override
  public OutputFile newOutputFile(String path) {
    try {
      return ugi.doAs((PrivilegedExceptionAction<OutputFile>) () -> delegate.newOutputFile(path));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create OutputFile with user impersonation", e);
    }
  }

  @Override
  public void deleteFile(String path) {
    try {
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        delegate.deleteFile(path);
        return null;
      });
    } catch (Exception e) {
      throw new RuntimeException("Failed to delete file with user impersonation", e);
    }
  }

  @Override
  public Map<String, String> properties() {
    try {
      return ugi.doAs((PrivilegedExceptionAction<Map<String, String>>) () -> delegate.properties());
    } catch (Exception e) {
      throw new RuntimeException("Failed to get properties with user impersonation", e);
    }
  }

  @Override
  public void initialize(Map<String, String> properties) {
    try {
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        delegate.initialize(properties);
        return null;
      });
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize with user impersonation", e);
    }
  }

  @Override
  public void close() {
    try {
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        delegate.close();
        return null;
      });
    } catch (Exception e) {
      throw new RuntimeException("Failed to close with user impersonation", e);
    }
  }
}

