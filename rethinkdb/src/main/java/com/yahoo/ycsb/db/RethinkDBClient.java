/**
 *  RethinkDB client binding for YCSB
 */

package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import com.rethinkdb.gen.ast.ReqlExpr;

public class RethinkDBClient extends DB {
  private Connection conn;
  public static final RethinkDB R = RethinkDB.r;

  private static final String DATABASE = "ycsb";

  // TODO this should be available from config
  private static final String TABLE = "usertable";
  
  /*
   * In case there are multiple hosts, we use this to pick one for each thread
   * round-robin.
   */
  private static int hostSelector = 0;
  private synchronized String pickHost(String[] hosts) {
    int myHost = hostSelector % hosts.length;
    hostSelector = myHost + 1;
    return hosts[myHost];
  }
  
  /*
   * Checks if the specified database and table exist. If not, creates them.
   * This method is synchronized to avoid race-conditions when inserting with
   * multiple threads.
   */
  private synchronized void maybeCreateTable(String durability) throws Exception {
    // TODO: The plucks in here are to work-around a bug in the Java driver
    //  that would crash on `null` fields in the query result. This bug
    //  has since been fixed. Update the Java driver and remove the `pluck`s.
    // TODO: Also we should check the return values of these queries.

    // Create database and table if not already there
    List<String> dbs = (List<String>)R.dbList().run(this.conn);
    if (!dbs.contains(DATABASE)) {
      R.dbCreate(DATABASE).pluck("dbs_created").run(this.conn);
    }

    List<String> tbls = (List<String>)R.db(DATABASE).tableList().run(this.conn);
    if (!tbls.contains(TABLE)) {
      ((ReqlExpr)(R.db(DATABASE).tableCreate(TABLE)
                                .optArg("primary_key", "__pk__")
                                .optArg("durability", durability)))
                 .pluck("tables_created").run(this.conn);
    }
    
    // If the table has already been created, wait until it becomes ready.
    R.db(DATABASE).table(TABLE).wait_().run(this.conn);
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
    int port = Integer.parseInt(config.getProperty("rethinkdb.port", "28015"));
    final String durability =
      config.getProperty("rethinkdb.durability", "hard");

    try {
      this.conn = R.connection().hostname(host).port(port).connect();

      maybeCreateTable(durability);
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
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  public int read(String table,
                  String key, Set<String> fields,
                  HashMap<String, ByteIterator> result) {
    // (pluck (get (table `table`) `key`) `fields`)
    try {
      ReqlExpr q = R.db(DATABASE).table(table).get(key);
      if (fields != null) {
        q = q.pluck(fields.toArray());
      }
      Map<String, String> out = (Map<String, String>)q.run(this.conn);
      StringByteIterator.putAllAsByteIterators(result, out);
      return 0;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return 1;
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
   * @return Zero on success, a non-zero error code on error.  See this class's
   * description for a discussion of error codes.
   */
  public int scan(String table,
                  String startkey,
                  int recordcount,
                  Set<String> fields,
                  Vector<HashMap<String, ByteIterator>> result) {
    // (pluck (limit (between (table `table`) `startkey` null) `recordcount`)
    // `fields`)
    try {
      Cursor<Map<String, String>> out =
        (Cursor<Map<String, String>>)R.db(DATABASE)
                                      .table(table)
                                      .between(startkey, R.maxval())
                                      .limit(recordcount)
                                      .pluck(fields)
                                      .run(this.conn);
      for (Map<String, String> row : out) {
        HashMap<String, ByteIterator> r2 = new HashMap<String, ByteIterator>();
        StringByteIterator.putAllAsByteIterators(r2, row);
        result.add(r2);
      }
      return 0;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return 1;
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
   * @return Zero on success, a non-zero error code on error.  See this class's
   * description for a discussion of error codes.
   */
  public int update(String table,
                    String key,
                    HashMap<String, ByteIterator> values) {
    // (update (get (table `table`) `key`) `values`)
    // TODO: Check for successful update?
    try {
      Map<String, String> obj = new HashMap<String, String>();
      StringByteIterator.putAllAsStrings(obj, values);

      R.db(DATABASE).table(table).get(key).update(obj).run(this.conn);
      return 0;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return 1;
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
   * @return Zero on success, a non-zero error code on error.  See this class's
   * description for a discussion of error codes.
   */
  public int insert(String table,
                    String key,
                    HashMap<String, ByteIterator> values) {
    // (insert (table `table`) `values`)
    // TODO: Check for successful insertion?
    try {
      Map<String, String> obj = new HashMap<String, String>();
      StringByteIterator.putAllAsStrings(obj, values);
      obj.put("__pk__", key); // Insert primary key
      R.db(DATABASE).table(table).insert(obj).run(this.conn);
      return 0;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return 1;
    }
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error.  See this class's
   * description for a discussion of error codes.
   */
  public int delete(String table, String key) {
    // (delete (get (table `table`) `key`))
    // TODO: Check for successful deletion?
    try {
      R.db(DATABASE).table(table).get(key).delete().run(this.conn);
      return 0;
    } catch (Exception e) {
      e.printStackTrace(System.err);
      return 1;
    }
  }
};
