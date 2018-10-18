/**
 * Copyright (c) 2013 - 2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import org.junit.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeNoException;

public class RethinkDBClientTest {
  private static final int RETHINKDB_DEFAULT_PORT = 28015;

  private final static RethinkDBClient client = new RethinkDBClient();
  private final static HashMap<String, ByteIterator> TEST_DATA;
  private final static String TEST_TABLE = "TEST_TABLE";
  private final static String TEST_KEY_0 = "0";
  private final static String TEST_KEY_1 = "1";
  private final static String TEST_KEY_2 = "2";
  private final static int TEST_DATA_SIZE = 10;

  static {
    TEST_DATA = new HashMap<>(TEST_DATA_SIZE);
    for (int i = 1; i <= TEST_DATA_SIZE; i++) {
      TEST_DATA.put("field_" + i, new StringByteIterator("value_" + i));
    }
  }

  /**
   * Verifies that the RethinkDB process or any other process is running and
   * that port 28015 is bound to it.
   *
   * If not, the tests are skipped.
   */
  @BeforeClass
  public static void setUpClass() throws DBException {
    Socket socket = null;
    try {
      socket = new Socket(InetAddress.getLocalHost(), RETHINKDB_DEFAULT_PORT);
      assertNotEquals("Socket is not bound.",-1, socket.getLocalPort());
    } catch (IOException ioException ) {
      String message = String.format("RethinkDB is not running and listening on port %d. Skipping tests.",
          RETHINKDB_DEFAULT_PORT);
      assumeNoException(message, ioException);
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      }
    }

    final Properties properties = new Properties();
    properties.put("rethinkdb.table", TEST_TABLE);
    client.setProperties(properties);
    client.init();
  }

  @AfterClass
  public static void tearDownClass() throws DBException {
    client.cleanup();
  }

  @Before
  public void setUp() {
    client.insert(TEST_TABLE, TEST_KEY_1, TEST_DATA);
    client.insert(TEST_TABLE, TEST_KEY_2, TEST_DATA);
  }

  @After
  public void tearDown() {
    client.delete(TEST_TABLE, TEST_KEY_1);
    client.delete(TEST_TABLE, TEST_KEY_2);
  }

  /**
   * Tests {@link RethinkDBClient#insert(String, String, Map)}.
   */
  @Test
  public void testInsert() {
    Status result = client.insert(TEST_TABLE, TEST_KEY_0, TEST_DATA);
    assertEquals(Status.OK, result);
  }

  /**
   * Tests {@link RethinkDBClient#delete(String, String)}.
   */
  @Test
  public void testDelete() {
    Status result = client.delete(TEST_TABLE, TEST_KEY_0);
    assertEquals(Status.OK, result);
  }

  /**
   * Tests {@link RethinkDBClient#read(String, String, Set, Map)}.
   */
  @Test
  public void testRead() {
    Set<String> keySet = TEST_DATA.keySet();
    HashMap<String, ByteIterator> docs = new HashMap<>(TEST_DATA_SIZE);
    Status result = client.read(TEST_TABLE, TEST_KEY_1, keySet, docs);
    assertEquals(Status.OK, result);
  }

  /**
   * Tests {@link RethinkDBClient#update(String, String, Map)}.
   */
  @Test
  public void testUpdate() {
    int i;

    HashMap<String, ByteIterator> newValues = new HashMap<>(TEST_DATA_SIZE);

    for (i = 1; i <= 10; i++) {
      newValues.put("field_" + i, new StringByteIterator("newValue_" + i));
    }

    Status result = client.update(TEST_TABLE, TEST_KEY_1, newValues);
    assertEquals(Status.OK, result);

    // Confirm that the values changed
    HashMap<String, ByteIterator> resultParam = new HashMap<>(TEST_DATA_SIZE);
    client.read(TEST_TABLE, TEST_KEY_1, TEST_DATA.keySet(), resultParam);

    for (i = 1; i <= 10; i++) {
      assertEquals("newValue_" + i, resultParam.get("field_" + i).toString());
    }
  }


  /**
   * Tests {@link RethinkDBClient#scan(String, String, int, Set, Vector)}.
   */
  @Test
  public void testScan() {
    Set<String> docs = TEST_DATA.keySet();
    Vector<HashMap<String, ByteIterator>> resultParam = new Vector<>(TEST_DATA_SIZE);
    Status result = client.scan(TEST_TABLE, TEST_KEY_1, TEST_DATA_SIZE, docs, resultParam);
    assertEquals(Status.OK, result);
  }
}
