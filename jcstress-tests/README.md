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

# Polaris Concurrency Tests

## Overview

This module contains concurrency tests for Polaris using the Java Concurrency Stress testing framework (jcstress). These tests are designed to detect synchronization issues, race conditions, and other concurrency problems that are difficult to find through traditional unit testing.

## What is JCStress?

JCStress is a testing framework developed by Oracle that helps verify correct behavior of concurrent Java code. Unlike traditional unit tests that run sequentially, jcstress tests:

- Execute code fragments in multiple threads simultaneously
- Run tests many times with different thread interleavings
- Detect race conditions and memory consistency issues
- Report observed outcomes and their frequencies

## Running the Tests

To run the jcstress tests, use the following Gradle command:

```bash
./gradlew :polaris-jcstress-tests:jcstress
```

The tests will run for several minutes, and results will be generated in HTML format at:
`polaris-jcstress-tests/build/reports/jcstress/index.html`

## Understanding Test Results

JCStress test results are presented in a table format showing:

- Observed outcomes and their frequencies
- Whether each outcome is expected or forbidden
- Total number of iterations
- Time taken for the test

A "FAILED" result means that a forbidden outcome was observed, indicating a potential concurrency issue.

## Writing JCStress Tests

JCStress tests can be written in two main styles:

### Style 1: Using an Arbiter

This style uses a separate method to observe the final state:

1. Define shared state variables
2. Create methods annotated with `@Actor` that will run concurrently
3. Define an `@Arbiter` method to check the final state
4. Use `@JCStressTest` and `@Outcome` annotations to specify expected results

Example:
```java
@JCStressTest
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Normal outcome")
@Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "Race condition")
@State
public class ArbiterTest {
    int x;

    @Actor
    void actor1() {
        x = 1;
    }

    @Actor
    void actor2() {
        x = 1;
    }

    @Arbiter
    void arbiter(I_Result r) {
        r.r1 = x;
    }
}
```

### Style 2: Direct Result Reporting

Actors can directly report results by accepting a result parameter:

1. Define shared state variables
2. Create methods annotated with `@Actor` that accept a result parameter
3. Each actor records its observations directly
4. Use `@JCStressTest` and `@Outcome` annotations to specify expected results

Example:
```java
@JCStressTest
@Outcome(id = "0, 1", expect = Expect.ACCEPTABLE, desc = "Thread 2 sees the write")
@Outcome(id = "0, 0", expect = Expect.ACCEPTABLE, desc = "Thread 2 doesn't see the write")
@State
public class DirectReportingTest {
    int x;

    @Actor
    void actor1() {
        x = 1;
    }

    @Actor
    void actor2(II_Result r) {
        r.r1 = 0;
        r.r2 = x; // Records what this thread observes
    }
}
```

Choose the style based on your test needs:
- Use an arbiter when you need to observe the final state after all actors complete
- Use direct reporting when actors need to record observations during their execution
- Direct reporting is particularly useful for testing visibility and ordering guarantees

## Common Concurrency Issues

Our jcstress tests help identify several types of concurrency problems:

1. **Race Conditions**: When multiple threads access shared data without proper synchronization
2. **Memory Visibility**: When changes made by one thread are not visible to other threads
3. **Atomicity Violations**: When operations that should be atomic can be interrupted
4. **Ordering Issues**: When operations execute in unexpected orders due to compiler or CPU reordering

## Best Practices

When working with jcstress tests:

- Keep tests focused on a single concurrency aspect
- Use meaningful names that describe the scenario being tested
- Document expected outcomes and their reasoning
- Run tests multiple times, as issues may not appear in every run
- Review test results carefully, especially for tests with multiple possible valid outcomes

## Resources

- [JCStress Official Documentation](https://wiki.openjdk.java.net/display/CodeTools/jcstress)
- [Java Memory Model Specification](https://docs.oracle.com/javase/specs/jls/se8/html/jls-17.html#jls-17.4)
- [Doug Lea's JSR-133 Cookbook](http://gee.cs.oswego.edu/dl/jmm/cookbook.html)
