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
package org.apache.polaris.apprunner.common;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class TestInputBuffer {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  void emptyInput() {
    var lines = new ArrayList<String>();
    var buf = new InputBuffer(new StringReader(""), lines::add);
    soft.assertThat(buf.io()).isFalse();
    soft.assertThat(lines).isEmpty();
    buf.flush();
    soft.assertThat(lines).isEmpty();
  }

  @Test
  void scattered() {
    var characters = new ArrayBlockingQueue<Integer>(250);

    // Just need some Reader implementation that implements ready() + read()
    var reader =
        new StringReader("") {
          @Override
          public int read() {
            return characters.poll();
          }

          @Override
          public boolean ready() {
            return characters.size() > 0;
          }
        };

    var lines = new ArrayList<String>();
    var buf = new InputBuffer(reader, lines::add);
    soft.assertThat(buf.io()).isFalse();
    soft.assertThat(lines).isEmpty();

    for (var c : "Hello World".toCharArray()) {
      characters.add((int) c);
    }

    // It should have done some I/O ...
    soft.assertThat(buf.io()).isTrue();
    // ... but there was no trailing newline, so nothing to print (yet)
    soft.assertThat(lines).isEmpty();

    for (var c : "\nFoo Bar Baz\nMeep".toCharArray()) {
      characters.add((int) c);
    }

    // It should have done some I/O ...
    soft.assertThat(buf.io()).isTrue();
    // ... and give us the first two lines ("Meep" is on an unterminated line)
    soft.assertThat(lines).containsExactly("Hello World", "Foo Bar Baz");

    // Just a CR does not trigger a "line complete"
    characters.add(13);
    soft.assertThat(buf.io()).isTrue();
    soft.assertThat(lines).containsExactly("Hello World", "Foo Bar Baz");

    // ... but a LF does
    characters.add(10);
    soft.assertThat(buf.io()).isTrue();
    soft.assertThat(lines).containsExactly("Hello World", "Foo Bar Baz", "Meep");

    // Add some more data, with an unterminated line...
    for (char c : "\nMore text\nNo EOL".toCharArray()) {
      characters.add((int) c);
    }
    soft.assertThat(buf.io()).isTrue();
    soft.assertThat(lines).containsExactly("Hello World", "Foo Bar Baz", "Meep", "", "More text");

    // "Final" flush() should yield the remaining data
    buf.flush();
    soft.assertThat(lines)
        .containsExactly("Hello World", "Foo Bar Baz", "Meep", "", "More text", "No EOL");
  }
}
