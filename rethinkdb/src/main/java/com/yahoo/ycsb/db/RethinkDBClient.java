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

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.*;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.Table;


/**
 * RethinkDB binding for the YCSB framework using the RethinkDB Java
 * <a href="https://rethinkdb.com/api/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 *
 * @see <a href="https://rethinkdb.com/api/java/">RethinkDB Java driver</a>
 */
public class RethinkDBClient extends DB {
  private Connection conn;
  private String readMode;
  private static final RethinkDB R = RethinkDB.r;

  // Defaults
  private static final String DATABASE = "ycsb";
  private static final String PORT = "28015";
  private static final String DURABILITY = "hard";
  private static final String TABLE = "usertable";


  /**
   * Put all of the entries of one map into the other, converting
   * String values into ByteIterator values.
   */
  private static void putAllAsByteIterators(Map<String, ByteIterator> out, Map<String, Object> in) {
    for (Map.Entry<String, Object> entry : in.entrySet()) {
      out.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()));
    }
  }

  /**
   * In case there are multiple hosts, we use this to pick one for each thread
   * round-robin.
   */
  private static int hostSelector = 0;
  private synchronized String pickHost(String[] hosts) {
    int myHost = hostSelector % hosts.length;
    hostSelector = myHost + 1;
    return hosts[myHost];
  }

  /**
   * Checks if the specified database and table exist. If not, creates them.
   * This method is synchronized to avoid race-conditions when inserting with
   * multiple threads.
   */
  private synchronized void maybeCreateTable(String durability, String table) {
    // Create database and table if not already there
    List<String> dbs = R.dbList().run(this.conn);
    if (!dbs.contains(DATABASE)) {
      R.dbCreate(DATABASE).run(this.conn);
    }

    List<String> tbls = R.db(DATABASE).tableList().run(this.conn);
    if (!tbls.contains(table)) {
      ((R.db(DATABASE).tableCreate(table)
          .optArg("primary_key", "__pk__")
          .optArg("durability", durability)))
          .run(this.conn);
    }

    // If the table has already been created, wait until it becomes ready.
    R.db(DATABASE).table(table).wait_().run(this.conn);
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    Properties config = getProperties();
    String hostString = config.getProperty("rethinkdb.host", "localhost");
    String[] hosts = hostString.split(",");
    String host = pickHost(hosts);

    final int port = Integer.parseInt(config.getProperty("rethinkdb.port", PORT));
    final String durability = config.getProperty("rethinkdb.durability", DURABILITY);
    final String table = config.getProperty("rethinkdb.table", TABLE);

    this.readMode = config.getProperty("rethinkdb.readmode", null);

    try {
      this.conn = R.connection().hostname(host).port(port).connect();

      maybeCreateTable(durability, table);
    } catch (Exception e) {
      e.printStackTrace(System.err);
      throw new DBException(e.getMessage());
    }
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    try {
      this.conn.close();
    } catch (Exception e) {
      throw new DBException(e.getMessage());
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Status.OK on success, Status.ERROR on error or Status.NOT_FOUND.
   */
  @Override
  public Status read(final String table,
                     final String key, Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      Table t = R.db(DATABASE).table(table);
      if (readMode != null) {
        t = t.optArg("read_mode", readMode);
      }
      ReqlExpr q = t.get(key);
      if (fields != null) {
        q = q.pluck(fields.toArray());
      }

      Map<String, Object> out = q.run(this.conn);

      putAllAsByteIterators(result, out);

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value
   * pairs for one record
   * @return Status.OK on success, Status.ERROR on error or Status.NOT_FOUND.
   */
  @Override
  public Status scan(final String table,
                     final String startkey,
                     int recordcount,
                     final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    try {
      Cursor<Map<String, Object>> cursor = R.db(DATABASE)
          .table(table)
          .between(startkey, R.maxval())
          .limit(recordcount)
          .pluck(fields.toArray())
          .run(this.conn);

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.NOT_FOUND;
      }

      for (Map<String, Object> row : cursor) {
        HashMap<String, ByteIterator> r2 = new HashMap<String, ByteIterator>();
        putAllAsByteIterators(r2, row);
        result.add(r2);
      }

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return Status.ERROR;
    }
  }


  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Status.OK on success, Status.ERROR on error.
   */
  @Override
  public Status update(final String table,
                       final String key,
                       final Map<String, ByteIterator> values) {
    try {
      Map<String, String> obj = new HashMap<String, String>();
      StringByteIterator.putAllAsStrings(obj, values);

      Map<String, Object> result = R.db(DATABASE).table(table).get(key).update(obj).run(this.conn);
      final long replaced = (long) result.get("replaced");
      return replaced == 1 ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return Status.ERROR;
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified
   * record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Status.OK on success, Status.ERROR on error.
   */
  @Override
  public Status insert(final String table,
                       final String key,
                       final Map<String, ByteIterator> values) {
    try {
      Map<String, String> obj = new HashMap<>();
      StringByteIterator.putAllAsStrings(obj, values);

      // Insert primary key
      obj.put("__pk__", key);
      Map<String, Object> result = R.db(DATABASE).table(table).insert(obj).run(this.conn);

      final long inserted = (long) result.get("inserted");
      return inserted == 1 ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return Status.ERROR;
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Status.OK on success, Status.ERROR on error or Status.NOT_FOUND.
   */
  public Status delete(final String table, final String key) {
    try {
      Map<String, Object> result = R.db(DATABASE).table(table).get(key).delete().run(this.conn);
      final long deleted = (long) result.get("deleted");
      return deleted == 1 ? Status.OK : Status.ERROR;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return Status.ERROR;
    }
  }
}
