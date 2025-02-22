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

package org.apache.polaris.service.events;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
@Identifier("test")
public class TestPolarisEventListener implements PolarisEventListener {
    private final List<Object> HISTORY = new ArrayList<>();

    public void clear() {
        HISTORY.clear();
        assertEmpty();
    }

    public void assertEmpty() {
        if(!HISTORY.isEmpty()) {
            throw new AssertionError("The event history isn't empty");
        }
    }

    public <T> T getLatest() {
        if(HISTORY.isEmpty()) {
            throw new AssertionError("There are no events");
        }
        Object lastEvent = HISTORY.getLast();
        return (T)lastEvent;
    }

    @Override
    public void onBeforeRequestRateLimited(BeforeRequestRateLimitedEvent event) {
        HISTORY.add(event);
    }

    @Override
    public void onBeforeTableCommit(BeforeTableCommitEvent event) {
        HISTORY.add(event);
    }

    @Override
    public void onAfterTableCommit(AfterTableCommitEvent event) {
        HISTORY.add(event);
    }

    @Override
    public void onBeforeViewCommit(BeforeViewCommitEvent event) {
        HISTORY.add(event);
    }

    @Override
    public void onAfterViewCommit(AfterViewCommitEvent event) {
        HISTORY.add(event);
    }

    @Override
    public void onBeforeRefreshTable(BeforeRefreshTableEvent event) {
        HISTORY.add(event);
    }

    @Override
    public void onAfterRefreshTable(AfterRefreshTableEvent event) {
        HISTORY.add(event);
    }

    @Override
    public void onBeforeRefreshView(BeforeRefreshViewEvent event) {
        HISTORY.add(event);
    }

    @Override
    public void onAfterRefreshView(AfterRefreshViewEvent event) {
        HISTORY.add(event);
    }

    @Override
    public void onBeforeAttemptTask(BeforeAttemptTaskEvent event) {
        HISTORY.add(event);
    }

    @Override
    public void onAfterAttemptTask(AfterAttemptTaskEvent event) {
        HISTORY.add(event);
    }
}
