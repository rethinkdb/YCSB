package com.yahoo.ycsb.generator.soe;


import com.yahoo.ycsb.workloads.CoreWorkload;
import net.spy.memcached.FailureMode;
import net.spy.memcached.internal.OperationFuture;

import java.util.Properties;

//import java.net.InetSocketAddress;



/**
 * Created by oleksandr.gyryk on 3/20/17.
 *
 * The storage-based generator is fetching pre-generated values/documents from an internal in-memory database instead
 * of generating new random values on the fly.
 * This approach allows YCSB to operate with real (or real-looking) JSON documents rather then synthetic.
 *
 * It also provides the ability to query rich JSON documents by splitting JSON documents into query predicates
 * (field, value, type, field-value relation, logical operation)
 */
public class MemcachedGenerator extends Generator {

  private net.spy.memcached.MemcachedClient client;

  public MemcachedGenerator(Properties p, String memHost, String memPort, String totalDocs) throws Exception {

    super(p);
    try {
      client = createMemcachedClient(memHost, Integer.parseInt(memPort));
      String prefix = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER;

      if (client.get(prefix + SOE_SYSTEMFIELD_TOTALDOCS_COUNT) == null){
        client.add(prefix + SOE_SYSTEMFIELD_TOTALDOCS_COUNT, 0, totalDocs);
      }
      int insertOffset = Integer.parseInt(p.getProperty(CoreWorkload.INSERT_START_PROPERTY,
          CoreWorkload.INSERT_START_PROPERTY_DEFAULT));

      if (client.get(prefix + SOE_SYSTEMFIELD_INSERTDOC_COUNTER) == null) {
        client.add(prefix + SOE_SYSTEMFIELD_INSERTDOC_COUNTER, 0,
            String.valueOf(Integer.parseInt(totalDocs) + 1 + insertOffset));
      }

      if (client.get(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_CUSTOMER) == null) {
        client.add(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_CUSTOMER, 0, "0");
      }

      prefix =  SOE_DOCUMENT_PREFIX_ORDER + SOE_SYSTEMFIELD_DELIMITER;

      if (client.get(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_ORDER) == null) {
        client.add(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_ORDER, 0, "0");
      }

    } catch (Exception e) {
      System.err.println("Memcached init error" + e.getMessage());
      throw e;
    }

  }

  protected net.spy.memcached.MemcachedClient createMemcachedClient(String memHost, int memPort)
      throws Exception {
    String address = memHost + ":" + memPort;
    return new net.spy.memcached.MemcachedClient(
        new net.spy.memcached.ConnectionFactoryBuilder().setDaemon(true).setFailureMode(FailureMode.Retry).build(),
        net.spy.memcached.AddrUtil.getAddresses(address));
  }


  @Override
  protected void setVal(String key, String value) {
    try {
      OperationFuture<Boolean> future = client.add(key, 0, value);
    } catch (Exception e) {
      System.err.println("error inserting value to memcached" + e.getMessage());
      throw e;
    }
  }

  @Override
  protected String getVal(String key) {
    try {
      return client.get(key).toString();
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  protected int increment(String key, int step) {
    try {
      return (int) client.incr(key, step);
    } catch (Exception e) {
      System.err.println("Error incrementing a counter in memcached" + e.getMessage());
      throw e;
    }
  }

}
