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

import com.dkhenry.RethinkDB.*;

public class RethinkDBClient extends DB {

    private RqlConnection conn;

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
        final String durability = config.getProperty("rethinkdb.durability", "hard");

        try {
            this.conn = RqlConnection.connect(host, port);

            // Create database and table if not already there
            //TODO move this check into the query language to
            //     eliminate the race on the db list when we
            //     do inserts with more than one thread.
            List<Object> dbs = this.conn.run(this.conn.db_list()).next().getList();
            if (!dbs.contains(DATABASE)) {
                this.conn.run(this.conn.db_create(DATABASE));
            }

            List<Object> tbls = this.conn.run(this.conn.db(DATABASE).table_list()).next().getList();
            if (!tbls.contains(TABLE)) {
                this.conn.run(this.conn.db(DATABASE).table_create(TABLE,
                    new HashMap() {{ put("primary_key","__pk__"); put("durability", durability); }}));
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
            RqlQuery q = this.conn.db(DATABASE).table(table).get(key);
            if (fields != null) {
                q = q.pluck(fields.toArray());
            }
            Map<String, Object> out = this.conn.run(q).next().getMap();
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
            RqlCursor out = this.conn.run(this.conn.db(DATABASE).table(table).between(startkey, null).limit(recordcount).pluck(fields));
            for (RqlObject row : out) {
                HashMap<String, ByteIterator> r2 = new HashMap<String, ByteIterator>();
                StringByteIterator.putAllAsByteIterators(r2, row.getMap());
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

            this.conn.run(this.conn.db(DATABASE).table(table).get(key).update(obj));
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
            this.conn.run(this.conn.db(DATABASE).table(table).insert(obj));
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
            this.conn.run(this.conn.db(DATABASE).table(table).get(key).delete());
            return 0;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return 1;
        }
    }
};
