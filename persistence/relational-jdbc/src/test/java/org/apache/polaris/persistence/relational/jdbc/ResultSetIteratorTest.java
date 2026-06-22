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
package org.apache.polaris.persistence.relational.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.junit.jupiter.api.Test;

public class ResultSetIteratorTest {

  @Test
  public void testIteration() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    Converter<String> converter = converterReturning("first", "second");

    when(resultSet.next()).thenReturn(true, true, false);

    ResultSetIterator<String> iterator = new ResultSetIterator<>(resultSet, converter);

    assertTrue(iterator.hasNext());
    assertEquals("first", iterator.next());

    assertTrue(iterator.hasNext());
    assertEquals("second", iterator.next());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void testNextThrowsWhenExhausted() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    Converter<String> converter = converterReturning();

    when(resultSet.next()).thenReturn(false);

    ResultSetIterator<String> iterator = new ResultSetIterator<>(resultSet, converter);

    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testToStream() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    Converter<String> converter = converterReturning("first", "second");

    when(resultSet.next()).thenReturn(true, true, false);

    ResultSetIterator<String> iterator = new ResultSetIterator<>(resultSet, converter);

    List<String> results = iterator.toStream().toList();

    assertEquals(List.of("first", "second"), results);
  }

  private static Converter<String> converterReturning(String... values) {
    return new Converter<String>() {
      private int index = 0;

      @Override
      public String fromResultSet(ResultSet rs) {
        return values[index++];
      }

      @Override
      public Map<String, Object> toMap(DatabaseType databaseType) {
        throw new UnsupportedOperationException("read-only converter stub");
      }
    };
  }
}
