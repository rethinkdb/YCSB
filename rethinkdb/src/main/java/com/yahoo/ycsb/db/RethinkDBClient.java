/**
 *  RethinkDB client binding for YCSB
 */

package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import com.rethinkdb.Connection;
import com.rethinkdb.Term;

public class RethinkDBClient extends DB {

    private Connection conn;
    boolean no_reply;

    private static final String DATABASE = "ycsb";

    // TODO this should be available from config
    private static final String TABLE = "usertable";

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
        Properties config = getProperties();
        String host = config.getProperty("rethinkdb.host", "localhost");
        int port = Integer.parseInt(config.getProperty("rethinkdb.port", "28015"));
        String durability = config.getProperty("rethinkdb.durability", "hard");
        no_reply = Boolean.parseBoolean(config.getProperty("rethinkdb.no_reply", "false"));
        
        try {
            this.conn = new Connection(host, port, DATABASE);

            // Create database and table if not already there
            List<String> dbs = Term.db_list().run(this.conn);
            if (!dbs.contains(DATABASE)) {
                Term.db_create(DATABASE).run(this.conn);
            }

            List<String> tbls = Term.table_list().run(this.conn);
            if (!tbls.contains(TABLE)) {
                Term.table_create(TABLE)
                    .addOption("primary_key", "__pk__")
                    .addOption("durability", durability)
                    .run(this.conn);
            }
        } catch (Exception e) {
            throw new DBException(e.getMessage());
        }
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
        try {
            this.conn.close();
        } catch (Exception e) {
            throw new DBException(e.getMessage());
        }
	}

	/**
	 * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error or "not found".
	 */
	public int read(String table, String key, Set<String> fields, HashMap<String,ByteIterator> result) {
        // (pluck (get (table `table`) `key`) `fields`)
        try {
            Map<String, String> out = Term.table(table).get(key).pluck(fields).run(conn);
            StringByteIterator.putAllAsByteIterators(result, out);
            return 0;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return 1;
        }
    }

	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result) {
        // (pluck (limit (between (table `table`) `startkey` null) `recordcount`) `fields`)
        try {
            List<Map<String, String>> out = Term.table(table).between(startkey, null).limit(recordcount).pluck(fields).run(conn);
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
	 * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key, overwriting any existing values with the same field name.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to write.
	 * @param values A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int update(String table, String key, HashMap<String,ByteIterator> values) {
        // (update (get (table `table`) `key`) `values`)
        try {
            Map<String, String> obj = new HashMap<String, String>();
            StringByteIterator.putAllAsStrings(obj, values);

            Term t = Term.table(table).get(key).update(obj);
            if (no_reply) {
                t.run_noreply(conn);
            } else {
                t.run(conn);
            }
            return 0;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return 1;
        }
    }

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int insert(String table, String key, HashMap<String,ByteIterator> values) {
        // (insert (table `table`) `values`)
        try {
            Map<String, String> obj = new HashMap<String, String>();
            StringByteIterator.putAllAsStrings(obj, values);
            obj.put("__pk__", key); // Insert primary key
            Term t = Term.table(table).insert(obj);
            if (no_reply) {
                t.run_noreply(conn);
            } else {
                t.run(conn);
            }
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
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public int delete(String table, String key) {
        // (delete (get (table `table`) `key`))
        try {
            Term t = Term.table(table).get(key).delete();
            if (no_reply) {
                t.run_noreply(conn);
            } else {
                t.run(conn);
            }
            return 0;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return 1;
        }
    }
};
