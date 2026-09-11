<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
   http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Polaris Asynchronous & Reliable Tasks

Cluster-wide task execution plus delayed and recurring scheduling, including restarts of "lost" and retries of failed
tasks.

The "caller facing" API is literally just a single `submit()` function that is called with the _kind_ of operation and
the parameters. The returned value provides a `CompletionStage`, which can be used - it is not required to subscribe to
it (fire and forget).

## Design

Task operations are called _task behaviors_.
_Task behaviors_ define the kind of operation that happens, the uniqueness of actual task executions,
the request and response parameter type and how retries are handled.

For example, "purging a table" is a _task behavior_, but also "cleanup the backend database" can be a _task behavior_.
The most important distinction between the two former examples is that there can be multiple "purge table" operations
(against different tables) but only one "cleanup the backend database" task instance.

A task is submitted via the `Tasks` interface, giving the `submit()` function the _task behavior_ and _task
parameters_. The returned object contains the _task ID_ and a `CompletionStage` to which call sites can subscribe to.
_Task functions_ can return a ("non-void") result, which can be further processed via the `CompletionState`.

Tasks are a global resource, not scoped to particular realms.

Task behaviors define failure/retry handling.
Failure handling happens when the JVM that started running a task did not write the execution result.
Such failures can happen when the executing node died or crashed.
Retry handling happens when a task execution failed, the task behavior defines the exact behavior of such retries.
Task behaviors may also define that a successful task is rescheduled (repeating task).

Despite that even successful tasks can be rescheduled, it is **not** a goal of this framework to have a fully-fledged
scheduling implementation with various repetition patterns (think: `cron`).

All tasks have a globally unique ID, which can be deterministic (i.e., computed from task parameters) for tasks that
depend on other values, or non-deterministic for one-off tasks. For example, a task that performs a maintenance
operation against a single global resource must have a deterministic, unique ID to ensure that no two instances of such
a task run concurrently. The _task behavior_ defines how the ID is generated.

The task framework ensures that no task is executed on more than one Polaris node at any time.
Shared persisted state object is used to coordinate operations against a task.
The former mentioned guarantee can only hold true if the wall clock is synchronized across all Polaris instances and
as long as there is no "split brain" situation in the backend database.

The task state is a composite of:

* the task ID (it's unique ID)
* the behavior ID (reference to _how_ a task behaves)
* the task status (see below)
* the task result (if successful)
* the error information (if failed)
* the next scheduled timestamp the task shall run
* the "recheck state" timestamp (see below)
* the "lost at" timestamp (see below)

The state of a task can be one of the following:

| Status    | Meaning                                                                      | Can transition to      |
|-----------|------------------------------------------------------------------------------|------------------------|
| `CREATED` | The task has been created but is not running anywhere nor did it run before. | `RUNNING` or deleted   |
| `RUNNING` | The task is currently running on a node.                                     | `SUCCESS` or `FAILURE` |
| `SUCCESS` | The task finished successfully. Its result, if present, is recorded.         | `RUNNING` or deleted   |
| `FAILURE` | The task threw an exception. Error information, if present, is recorded.     | `RUNNING` or deleted   |

Changes to a task state happen consistently.

Tasks, if eligible for being executed, have their _next scheduled timestamp_ attribute set. This attribute is mandatory
for `CREATED` tasks and optional for `SUCCESS` and `FAILURE` states.

Polaris nodes regularly fetch the set of tasks that are eligible to be executed.

A task to be executed gets its status changed to `RUNNING` and the attributes "recheck state not before" and "lost not
before" both holding timestamps, set.
"Recheck state not before" is an indicator when _other_ nodes can refresh the state of the task.
The value effectively says that the state is not expected to change before that timestamp.
The node executing the task either regularly updates the "recheck state not before" (and "lost not before") timestamps
or transitions the status to `SUCCESS` or `FAILURE`.
These updates must happen before "recheck state not before" minus an expected wall-clock-drift and internal delays.
The "lost not before" timestamp is meant to handle the case that a node that executes a task dies.
Another node that encounters a task with a "lost not before" timestamp in the past can pick it up for execution.

A task is eligible for execution if one of the following conditions is true:

* `status in (CREATED, FAILURE, SUCCESS) && scheduleNotBefore is present && scheduleNotBefore <= now()`
  (`scheduleNotBefore` is mandatory for `CREATED`)
* `status == RUNNING && lostNotBefore <= now()`

### Optimization for locally created tasks

Implementations _may_ opt to not persist the `CREATED` status for tasks that shall be executed immediately, but instead
persist the `RUNNING` state directly. This prevents other nodes from picking up the task.

## Requirements

As with probably all systems that rely on the wall-clock, it is essential that all Polaris nodes have their wall clock being
synchronized. Some reasonable amount of wall clock drift must be considered in every implementation.

## Code structure

The code is structured into multiple modules. Consuming code should almost always pull in only the API module.

* `polaris-tasks-api` provides the necessary Java interfaces and immutable types.
* `polaris-tasks-spi` behavior implementation interfaces.
* `polaris-tasks-store` storage-implementation, persistence agnostic.
* `polaris-tasks-store-meta` provides the storage implementation optimized for JDBC et al.
* `polaris-tasks-store-nosql` provides the storage implementation optimized for NoSQL.
* `polaris-tasks-impl-base` common code for client and server implementations.
* `polaris-tasks-impl-server` server side implementation, which can submit and execute tasks.
* `polaris-tasks-impl-client` client side implementation, which can only submit but not execute tasks.
* `polaris-tasks-impl-remote` a "execute remotely" implementation could go here.

## Persona

### Task requestor ("caller", "user")

1. Create a _task request_ that describes the task, the behavior (think: which implementation performs the work)
   and parameters.
2. Submit the _task request_, optionally subscribe to the result.

### Implementer

* Create a _behavior ID_
* Define task _parameter_ value type (Jackson serializable)
* Optional: Define task _result_ value type (Jackson serializable)
* Implement `TaskBehavior` and the `TaskFunction`

### Service instance

Provides a `Tasks` implementation, which can submit tasks and execute tasks.

### Client instance

Provides a `Tasks` implementation, which can submit tasks but not execute tasks. This kind is intended for client
application use cases, which should not run task executions from other (Polaris service) instances.
