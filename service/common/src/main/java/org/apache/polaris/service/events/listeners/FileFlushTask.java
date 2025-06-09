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

package org.apache.polaris.service.events.listeners;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoBufferUnderflowException;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.polaris.core.entity.PolarisEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileFlushTask implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileFlushTask.class);
  private final Kryo kryo = new Kryo();

  private final String file;
  private final String realmId;
  private final Consumer<String> cleanupFutures;
  private final BiConsumer<String, List<PolarisEvent>> eventWriter;

  public FileFlushTask(
      String file,
      String realmId,
      Consumer<String> cleanupFutures,
      BiConsumer<String, List<PolarisEvent>> eventWriter) {
    this.file = file;
    this.realmId = realmId;
    this.cleanupFutures = cleanupFutures;
    this.eventWriter = eventWriter;
    kryo.register(PolarisEvent.class);
    kryo.register(PolarisEvent.ResourceType.class);
  }

  @Override
  public void run() {
    LOGGER.trace("Starting file flush task #{}", file);
    List<PolarisEvent> polarisEvents = new ArrayList<>();
    try (Input in = new Input(new FileInputStream(this.file))) {
      while (true) {
        PolarisEvent polarisEvent = kryo.readObject(in, PolarisEvent.class);
        polarisEvents.add(polarisEvent);
      }
    } catch (KryoException e) {
      // Possibly end of file reached
      if (!(e.getCause() instanceof EOFException) && !(e instanceof KryoBufferUnderflowException)) {
        LOGGER.error("Failed to read events from file {}", file, e);
        this.cleanupFutures.accept(this.file);
        return;
      }
    } catch (IOException e) {
      if (e.getCause() instanceof FileNotFoundException) {
        LOGGER.trace("Skipping file {}, as it may no longer be present", file);
      }
    }

    try {
      if (!polarisEvents.isEmpty()) {
        // Write all events back to the metastore
        this.eventWriter.accept(this.realmId, polarisEvents);
      }
    } catch (RuntimeException e) {
      LOGGER.error(
          "Failed to write events to meta store. Leaving file for future cleanup tasks.", e);
      return;
    }

    // Delete file
    File file = new File(this.file);
    file.delete();
    this.cleanupFutures.accept(this.file);
  }
}
