package com.yahoo.ycsb.generator.soe;

import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.workloads.soe.SoeQueryPredicate;
import com.yahoo.ycsb.workloads.soe.SoeWorkload;

import java.util.*;
import org.json.*;


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
public abstract class Generator {

  private int totalDocsCount = 0;
  private int storedDocsCountCustomer = 0;
  private int storedDocsCountOrder = 0;
  private Random rand = new Random();

  private boolean allValuesInitialized = false;
  private Properties properties;
  private int queryLimitMin = 0;
  private int queryLimitMax = 0;
  private int queryOffsetMin = 0;
  private int queryOffsetMax = 0;

  private boolean isZipfian = false;
  private ZipfianGenerator zipfianGenerator = null;

  private SoeQueryPredicate soePredicate;
  private ArrayList<SoeQueryPredicate> soePredicatesSequence;


  public static final String SOE_DOCUMENT_PREFIX_CUSTOMER = "customer";
  public static final String SOE_DOCUMENT_PREFIX_ORDER = "order";

  public static final String SOE_SYSTEMFIELD_DELIMITER = ":::";
  public static final String SOE_SYSTEMFIELD_INSERTDOC_COUNTER = "SOE_insert_document_counter";
  public static final String SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_CUSTOMER = "SOE_storage_docs_count_customer";
  public static final String SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_ORDER = "SOE_storage_docs_count_order";
  public static final String SOE_SYSTEMFIELD_TOTALDOCS_COUNT = "SOE_total_docs_count";

  private static final String SOE_METAFIELD_DOCID = "SOE_doc_id";
  private static final String SOE_METAFIELD_INSERTDOC = "SOE_insert_document";

  private static final String SOE_FIELD_CUSTOMER_ID = "_id";
  private static final String SOE_FIELD_CUSTOMER_DOCID = "doc_id";
  private static final String SOE_FIELD_CUSTOMER_GID = "gid";
  private static final String SOE_FIELD_CUSTOMER_FNAME = "first_name";
  private static final String SOE_FIELD_CUSTOMER_LNAME = "middle_name";
  private static final String SOE_FIELD_CUSTOMER_MNAME = "last_name";
  private static final String SOE_FIELD_CUSTOMER_BALLANCE = "ballance_current";
  public static final String SOE_FIELD_CUSTOMER_DOB = "dob";
  private static final String SOE_FIELD_CUSTOMER_EMAIL = "email";
  private static final String SOE_FIELD_CUSTOMER_ISACTIVE = "isActive";
  private static final String SOE_FIELD_CUSTOMER_LINEARSCORE = "linear_score";
  private static final String SOE_FIELD_CUSTOMER_WEIGHTEDSCORE = "weighted_score";
  private static final String SOE_FIELD_CUSTOMER_PHONECOUNTRY = "phone_country";
  private static final String SOE_FIELD_CUSTOMER_PHONE = "phone_by_country";
  private static final String SOE_FIELD_CUSTOMER_AGEGROUP = "age_group";
  private static final String SOE_FIELD_CUSTOMER_AGE = "age_by_group";
  private static final String SOE_FIELD_CUSTOMER_URLPROTOCOL = "url_protocol";
  private static final String SOE_FIELD_CUSTOMER_URLSITE = "url_site";
  private static final String SOE_FIELD_CUSTOMER_URLDOMAIN = "url_domain";
  private static final String SOE_FIELD_CUSTOMER_URL = "url";
  private static final String SOE_FIELD_CUSTOMER_DEVICES = "devices";
  private static final String SOE_FIELD_CUSTOMER_LINKEDDEVICES = "linked_devices";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS = "address";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_STREET = "street";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_CITY = "city";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP = "zip";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY = "country";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION = "geo_region";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR = "prev_address";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_STREET = "street";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CITY = "city";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_COUNTRY = "country";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP = "zip";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER = "property_current_owner";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_FNAME = "first_name";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_MNAME = "middle_name";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_LNAME = "last_name";
  private static final String SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_PHONE = "phone";
  private static final String SOE_FIELD_CUSTOMER_CHILDREN = "children";
  private static final String SOE_FIELD_CUSTOMER_CHILDREN_OBJ_FNAME = "first_name";
  private static final String SOE_FIELD_CUSTOMER_CHILDREN_OBJ_GENDER = "gender";
  private static final String SOE_FIELD_CUSTOMER_CHILDREN_OBJ_AGE = "age";
  private static final String SOE_FIELD_CUSTOMER_VISITEDPLACES = "visited_places";
  private static final String SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY = "country";
  private static final String SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES = "cities";
  public static final String SOE_FIELD_CUSTOMER_ORDER_LIST = "order_list";

  private static final String SOE_FIELD_ORDER_ID = "_id";
  private static final String SOE_FIELD_ORDER_CUSTOMERID = "customer_id";
  private static final String SOE_FIELD_ORDER_YEAR = "year";
  private static final String SOE_FIELD_ORDER_MONTH = "month";
  private static final String SOE_FIELD_ORDER_DAY = "day";
  private static final String SOE_FIELD_ORDER_WEEKDAY = "weekday";
  private static final String SOE_FIELD_ORDER_QUANTITY = "quantity";
  private static final String SOE_FIELD_ORDER_LISTPRICE = "list_price";
  private static final String SOE_FIELD_ORDER_DISCOUNT = "discount_amount_percent";
  private static final String SOE_FIELD_ORDER_SALEPRICE = "sale_price";
  private static final String SOE_FIELD_ORDER_TAX = "tax";
  private static final String SOE_FIELD_ORDER_COUPON = "coupon";
  private static final String SOE_FIELD_ORDER_DEPARTMNET = "department";
  private static final String SOE_FIELD_ORDER_PRODUCTNAME = "product_name";


  private final Set<String> allFields = new HashSet<String>() {{
      add(SOE_FIELD_CUSTOMER_ID);
      add(SOE_FIELD_CUSTOMER_DOCID);
      add(SOE_FIELD_CUSTOMER_GID);
      add(SOE_FIELD_CUSTOMER_FNAME);
      add(SOE_FIELD_CUSTOMER_LNAME);
      add(SOE_FIELD_CUSTOMER_MNAME);
      add(SOE_FIELD_CUSTOMER_BALLANCE);
      add(SOE_FIELD_CUSTOMER_DOB);
      add(SOE_FIELD_CUSTOMER_EMAIL);
      add(SOE_FIELD_CUSTOMER_ISACTIVE);
      add(SOE_FIELD_CUSTOMER_LINEARSCORE);
      add(SOE_FIELD_CUSTOMER_WEIGHTEDSCORE);
      add(SOE_FIELD_CUSTOMER_PHONECOUNTRY);
      add(SOE_FIELD_CUSTOMER_PHONE);
      add(SOE_FIELD_CUSTOMER_AGEGROUP);
      add(SOE_FIELD_CUSTOMER_AGE);
      add(SOE_FIELD_CUSTOMER_URLPROTOCOL);
      add(SOE_FIELD_CUSTOMER_URLSITE);
      add(SOE_FIELD_CUSTOMER_URLDOMAIN);
      add(SOE_FIELD_CUSTOMER_URL);
      add(SOE_FIELD_CUSTOMER_DEVICES);
      add(SOE_FIELD_CUSTOMER_LINKEDDEVICES);
      add(SOE_FIELD_CUSTOMER_ADDRESS);
      add(SOE_FIELD_CUSTOMER_CHILDREN);
      add(SOE_FIELD_CUSTOMER_VISITEDPLACES);
      add(SOE_FIELD_CUSTOMER_ORDER_LIST);
    }};


  protected abstract void setVal(String key, String value);

  protected abstract String getVal(String key);

  protected abstract int increment(String key, int step);

  public Generator(Properties p) {
    properties = p;

    queryLimitMin = Integer.parseInt(p.getProperty(SoeWorkload.SOE_QUERY_LIMIT_MIN,
        SoeWorkload.SOE_QUERY_LIMIT_MIN_DEFAULT));
    queryLimitMax = Integer.parseInt(p.getProperty(SoeWorkload.SOE_QUERY_LIMIT_MAX,
        SoeWorkload.SOE_QUERY_LIMIT_MAX_DEFAULT));
    if (queryLimitMax < queryLimitMin) {
      int buff = queryLimitMax;
      queryLimitMax = queryLimitMin;
      queryLimitMin = buff;
    }

    queryOffsetMin = Integer.parseInt(p.getProperty(SoeWorkload.SOE_QUERY_OFFSET_MIN,
        SoeWorkload.SOE_QUERY_OFFSET_MIN_DEFAULT));
    queryOffsetMax = Integer.parseInt(p.getProperty(SoeWorkload.SOE_QUERY_OFFSET_MAX,
        SoeWorkload.SOE_QUERY_OFFSET_MAX_DEFAULT));
    if (queryOffsetMax < queryOffsetMin) {
      int buff = queryOffsetMax;
      queryOffsetMax = queryOffsetMin;
      queryOffsetMin = buff;
    }

    isZipfian = p.getProperty(SoeWorkload.SOE_REQUEST_DISTRIBUTION,
        SoeWorkload.SOE_REQUEST_DISTRIBUTION_DEFAULT).equals("zipfian");
  }

  public final Set<String> getAllFields() {
    return allFields;
  }

  public void putCustomerDocument(String docKey, String docBody) throws Exception {
    HashMap<String, String> tokens = tokenize(docBody);
    String prefix = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER;
    int storageCount = increment(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_CUSTOMER, 1) - 1;

    setVal(prefix + SOE_METAFIELD_DOCID + SOE_SYSTEMFIELD_DELIMITER + storageCount, docKey);
    setVal(prefix + SOE_METAFIELD_INSERTDOC + SOE_SYSTEMFIELD_DELIMITER + storageCount, docBody);

    for (String key : tokens.keySet()){
      String storageKey = prefix + key + SOE_SYSTEMFIELD_DELIMITER + storageCount;
      String value = tokens.get(key);
      if (value != null) {
        setVal(storageKey, value);
      }  else {
        for (int i = (storageCount-1); i>0; i--) {
          String prevKey = prefix + key + SOE_SYSTEMFIELD_DELIMITER + i;
          String prevVal = getVal(prevKey);
          if (prevVal != null) {
            setVal(storageKey, prevVal);
            break;
          }
        }
      }
    }

    //make sure all values are initialized
    if ((!allValuesInitialized) && (storageCount > 1)) {
      boolean nullDetected = false;
      for (String key : tokens.keySet()) {
        for (int i = 0; i< storageCount; i++) {
          String storageKey = prefix + key + SOE_SYSTEMFIELD_DELIMITER + i;
          String storageValue = getVal(storageKey);
          if (storageValue != null) {
            for (int j = i; j>=0; j--) {
              storageKey = prefix + key + SOE_SYSTEMFIELD_DELIMITER + j;
              setVal(storageKey, storageValue);
            }
            break;
          } else {
            nullDetected = true;
          }
        }
      }
      allValuesInitialized = !nullDetected;
    }
  }

  public void putOrderDocument(String docKey, String docBody) throws Exception {
    HashMap<String, String> tokens = tokenizeOrderFields(docBody);

    String prefix = SOE_DOCUMENT_PREFIX_ORDER + SOE_SYSTEMFIELD_DELIMITER;
    int storageCount = increment(prefix + SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_ORDER, 1) - 1;

    for (String key : tokens.keySet()){
      String storageKey = prefix + key + SOE_SYSTEMFIELD_DELIMITER + storageCount;
      String value = tokens.get(key);
      if (value != null) {
        setVal(storageKey, value);
      }  else {
        for (int i = (storageCount-1); i>0; i--) {
          String prevKey = prefix + key + SOE_SYSTEMFIELD_DELIMITER + i;
          String prevVal = getVal(prevKey);
          if (prevVal != null) {
            setVal(storageKey, prevVal);
            break;
          }
        }
      }
    }
  }


  public SoeQueryPredicate getPredicate() {
    return soePredicate;
  }

  public ArrayList<SoeQueryPredicate> getPredicatesSequence() {
    return soePredicatesSequence;
  }

  public void buildInsertDocument() {
    //String storageKey = buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_METAFIELD_INSERTDOC);
    String storageKey = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
        SOE_METAFIELD_INSERTDOC + SOE_SYSTEMFIELD_DELIMITER + getNumberRandom(getStoredCustomersCount());

    String docBody = getVal(storageKey);
    String keyPrefix = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER;
    int docCounter = increment(keyPrefix + SOE_SYSTEMFIELD_INSERTDOC_COUNTER, 1);

    soePredicate = new SoeQueryPredicate();
    soePredicate.setDocid(keyPrefix + docCounter);
    soePredicate.setValueA(docBody);

  }

  // building value as random to make sure the original value is overwritten with new one
  public void buildUpdatePredicate() {
    buildInsertDocument();
    SoeQueryPredicate queryPredicate = new SoeQueryPredicate();
    queryPredicate.setName(SOE_FIELD_CUSTOMER_BALLANCE);
    queryPredicate.setValueA("$" + rand.nextInt(99999) + "." + rand.nextInt(99));
    soePredicate.setNestedPredicateA(queryPredicate);
  }

  public void buildPagePredicate() {
    soePredicate = new SoeQueryPredicate();
    soePredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate innerPredicate = new SoeQueryPredicate();
    innerPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP);
    innerPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP)));
    soePredicate.setNestedPredicateA(innerPredicate);
  }

  public void buildPage2Predicate() {
    soePredicate = new SoeQueryPredicate();
    soePredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate innerPredicate = new SoeQueryPredicate();
    innerPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION);
    innerPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION)));
    soePredicate.setNestedPredicateA(innerPredicate);
  }


  public void buildSearchPredicatesSequenceN3() {
    SoeQueryPredicate predicate;
    soePredicatesSequence = new ArrayList<SoeQueryPredicate>();

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate innerPredicate = new SoeQueryPredicate();
    innerPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY);
    innerPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY)));
    predicate.setNestedPredicateA(innerPredicate);
    soePredicatesSequence.add(predicate);

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_AGEGROUP);
    predicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_AGEGROUP)));
    soePredicatesSequence.add(predicate);

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_DOB);
    String dob = getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_DOB));
    try {
      predicate.setValueA(dob.split("-")[0]);
    } catch (Exception e) {
      System.err.println("failed to get year out of DOB" + e.getMessage());
      predicate.setValueA("2017");
    }
    soePredicatesSequence.add(predicate);
  }


  public void buildSearch2PredicatesSequenceN3() {
    SoeQueryPredicate predicate;
    soePredicatesSequence = new ArrayList<SoeQueryPredicate>();

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate innerPredicate = new SoeQueryPredicate();
    innerPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION);
    innerPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION)));
    predicate.setNestedPredicateA(innerPredicate);
    soePredicatesSequence.add(predicate);

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_AGEGROUP);
    predicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_AGEGROUP)));
    soePredicatesSequence.add(predicate);

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_DOB);
    String dob = getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_DOB));
    try {
      predicate.setValueA(dob.split("-")[0]);
    } catch (Exception e) {
      System.err.println("failed to get year out of DOB" + e.getMessage());
      predicate.setValueA("2017");
    }
    soePredicatesSequence.add(predicate);
  }

  public void buildNestedScanPredicate() {
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    predicate.setNestedPredicateA(new SoeQueryPredicate());
    predicate.getNestedPredicateA().setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR);
    predicate.getNestedPredicateA().setNestedPredicateA(new SoeQueryPredicate());
    predicate.getNestedPredicateA().getNestedPredicateA().setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP);
    String key = buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER,
        SOE_FIELD_CUSTOMER_ADDRESS, SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP);
    predicate.getNestedPredicateA().getNestedPredicateA().setValueA(getVal(key));
    soePredicate = predicate;
  }

  public void buildArrayScanPredicate() {
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_DEVICES);
    predicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_DEVICES)));
    soePredicate = predicate;
  }

  public void buildArrayDeepScanPredicate() {
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_VISITEDPLACES);

    int storageKeyOffset = (isZipfian)? getNumberZipfianUnifrom() : getNumberRandom(getStoredCustomersCount());

    String storageKeyPrefix = SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
        SOE_FIELD_CUSTOMER_VISITEDPLACES + SOE_SYSTEMFIELD_DELIMITER;

    SoeQueryPredicate innerPredicateA = new SoeQueryPredicate();
    innerPredicateA.setName(SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY);
    innerPredicateA.setValueA(getVal(storageKeyPrefix +
        SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY + SOE_SYSTEMFIELD_DELIMITER + storageKeyOffset));

    SoeQueryPredicate innerPredicateB = new SoeQueryPredicate();
    innerPredicateB.setName(SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES);
    innerPredicateB.setValueA(getVal(storageKeyPrefix +
        SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES + SOE_SYSTEMFIELD_DELIMITER + storageKeyOffset));

    predicate.setNestedPredicateA(innerPredicateA);
    predicate.setNestedPredicateB(innerPredicateB);
    soePredicate = predicate;

  }

  public void buildReport1PredicateSequence() {

    soePredicatesSequence = new ArrayList<>();
    SoeQueryPredicate predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_ORDER_LIST);
    soePredicatesSequence.add(predicate);

    predicate = new SoeQueryPredicate();
    predicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate innerPredicate = new SoeQueryPredicate();
    innerPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP);
    innerPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP)));
    predicate.setNestedPredicateA(innerPredicate);
    soePredicatesSequence.add(predicate);
  }

  public void buildReport2PredicateSequence() {

    soePredicatesSequence = new ArrayList<>();

    String orderPrefix = SOE_DOCUMENT_PREFIX_ORDER + SOE_SYSTEMFIELD_DELIMITER;

    SoeQueryPredicate oDatePredicate = new SoeQueryPredicate();
    oDatePredicate.setName(SOE_FIELD_ORDER_MONTH);
    oDatePredicate.setValueA(getVal(orderPrefix + SOE_FIELD_ORDER_MONTH + SOE_SYSTEMFIELD_DELIMITER +
        getNumberRandom(getStoredOrdersCount())));
    soePredicatesSequence.add(oDatePredicate);

    SoeQueryPredicate oSalepricePredicate = new SoeQueryPredicate();
    oSalepricePredicate.setName(SOE_FIELD_ORDER_SALEPRICE);
    oSalepricePredicate.setValueA(getVal(orderPrefix + SOE_FIELD_ORDER_SALEPRICE + SOE_SYSTEMFIELD_DELIMITER +
        getNumberRandom(getStoredOrdersCount())));
    soePredicatesSequence.add(oSalepricePredicate);

    SoeQueryPredicate cAddressPredicate = new SoeQueryPredicate();
    cAddressPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS);
    SoeQueryPredicate cAddressZipPredicate = new SoeQueryPredicate();
    cAddressZipPredicate.setName(SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP);

    cAddressZipPredicate.setValueA(getVal(buildStorageKey(SOE_DOCUMENT_PREFIX_CUSTOMER, SOE_FIELD_CUSTOMER_ADDRESS,
        SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP)));
    cAddressPredicate.setNestedPredicateA(cAddressZipPredicate);
    soePredicatesSequence.add(cAddressPredicate);

    SoeQueryPredicate cOrderList = new SoeQueryPredicate();
    cOrderList.setName(SOE_FIELD_CUSTOMER_ORDER_LIST);
    soePredicatesSequence.add(cOrderList);
  }

  public void buildSyncPredicate() {
    // todo
  }



  public String getCustomerIdRandom() {
    return SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER + getNumberRandom(getTotalcustomersCount());
  }

  public String getCustomerIdWithDistribution() {
    if (isZipfian) {
      return SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
          getNumberZipfianLatests(getTotalcustomersCount());
    }
    return getCustomerIdRandom();
  }

  public int getRandomLimit(){
    if (queryLimitMax == queryLimitMin) {
      return queryLimitMax;
    }
    return rand.nextInt(queryLimitMax - queryLimitMin + 1) + queryLimitMin;
  }

  public int getRandomOffset(){

    if (queryOffsetMax == queryOffsetMin) {
      return queryOffsetMax;
    }
    return rand.nextInt(queryOffsetMax - queryOffsetMin + 1) + queryOffsetMin;
  }

  private String buildStorageKey(String token1) {
    if (isZipfian) {
      return token1 + SOE_SYSTEMFIELD_DELIMITER + getNumberZipfianUnifrom();
    }
    return token1 + SOE_SYSTEMFIELD_DELIMITER + getNumberRandom(getStoredCustomersCount());
  }


  private String buildStorageKey(String token1, String token2) {
    return token1 + SOE_SYSTEMFIELD_DELIMITER + buildStorageKey(token2);
  }

  private String buildStorageKey(String token1, String token2, String token3) {
    return token1 + SOE_SYSTEMFIELD_DELIMITER + buildStorageKey(token2, token3);
  }

  private String buildStorageKey(String token1, String token2, String token3, String token4) {
    return token1 + SOE_SYSTEMFIELD_DELIMITER + buildStorageKey(token2, token3, token4);
  }

  private String buildStorageKey(String token1, String token2, String token3, String token4, String token5) {
    return token1 + SOE_SYSTEMFIELD_DELIMITER + buildStorageKey(token2, token3, token4, token5);
  }



  private HashMap<String, String> tokenize(String jsonString) {
    HashMap<String, String> tokens = new HashMap<String, String>();
    JSONObject obj = new JSONObject(jsonString);

    try {
      tokenizeFields(obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - plain fields");
      ex.printStackTrace();
    }

    try {
      tokenizeArrays(obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - arrays");
      ex.printStackTrace();
    }

    try {
      tokenizeObjects(obj, tokens);
    } catch (JSONException ex) {
      System.err.println("Document parsing error - objects");
      ex.printStackTrace();
    }

    return tokens;
  }

  private HashMap<String, String>  tokenizeOrderFields(String jsonString) {
    HashMap<String, String> tokens = new HashMap<String, String>();
    JSONObject obj = new JSONObject(jsonString);

    //string
    ArrayList<String> stringFields = new ArrayList<>(Arrays.asList(SOE_FIELD_ORDER_ID, SOE_FIELD_ORDER_YEAR,
        SOE_FIELD_ORDER_MONTH, SOE_FIELD_ORDER_WEEKDAY, SOE_FIELD_ORDER_COUPON, SOE_FIELD_ORDER_DEPARTMNET,
        SOE_FIELD_ORDER_PRODUCTNAME, SOE_FIELD_ORDER_CUSTOMERID));

    for (String field : stringFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, obj.getString(field));
      }
    }

    //integer
    ArrayList<String> intFields = new ArrayList<>(Arrays.asList(SOE_FIELD_ORDER_QUANTITY, SOE_FIELD_ORDER_DISCOUNT,
        SOE_FIELD_ORDER_DAY));

    for (String field : intFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, String.valueOf(obj.getInt(field)));
      }
    }

    //float
    ArrayList<String> floatFields = new ArrayList<>(Arrays.asList(SOE_FIELD_ORDER_LISTPRICE,
        SOE_FIELD_ORDER_SALEPRICE, SOE_FIELD_ORDER_TAX));
    for (String field : floatFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, String.valueOf(obj.getDouble(field)));
      }
    }

    return tokens;
  }

  private void tokenizeFields(JSONObject obj, HashMap<String, String> tokens) {


    //string
    ArrayList<String> stringFields = new ArrayList<>(Arrays.asList(SOE_FIELD_CUSTOMER_ID,
        SOE_FIELD_CUSTOMER_GID, SOE_FIELD_CUSTOMER_FNAME, SOE_FIELD_CUSTOMER_LNAME, SOE_FIELD_CUSTOMER_MNAME,
        SOE_FIELD_CUSTOMER_BALLANCE, SOE_FIELD_CUSTOMER_DOB, SOE_FIELD_CUSTOMER_EMAIL, SOE_FIELD_CUSTOMER_PHONECOUNTRY,
        SOE_FIELD_CUSTOMER_PHONE, SOE_FIELD_CUSTOMER_AGEGROUP, SOE_FIELD_CUSTOMER_URLPROTOCOL,
        SOE_FIELD_CUSTOMER_URLSITE, SOE_FIELD_CUSTOMER_URLDOMAIN, SOE_FIELD_CUSTOMER_URL));

    for (String field : stringFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, obj.getString(field));
      }
    }

    //integer
    ArrayList<String> intFields = new ArrayList<>(Arrays.asList(SOE_FIELD_CUSTOMER_DOCID,
        SOE_FIELD_CUSTOMER_LINEARSCORE, SOE_FIELD_CUSTOMER_WEIGHTEDSCORE, SOE_FIELD_CUSTOMER_AGE));

    for (String field : intFields) {
      tokens.put(field, null);
      if (obj.has(field) && !obj.isNull(field)) {
        tokens.put(field, String.valueOf(obj.getInt(field)));
      }
    }

    //boolean
    String field = SOE_FIELD_CUSTOMER_ISACTIVE;
    tokens.put(field, null);
    if (obj.has(field) && !obj.isNull(field)) {
      tokens.put(field, String.valueOf(obj.getBoolean(field)));
    }


  }

  private void tokenizeArrays(JSONObject obj, HashMap<String, String>  tokens) {

    //array
    String field = SOE_FIELD_CUSTOMER_DEVICES;
    tokens.put(field, null);
    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray arr = obj.getJSONArray(field);
      if (arr.length() > 0) {
        int element = rand.nextInt(arr.length());
        tokens.put(field, arr.getString(element));
      }
    }

    field = SOE_FIELD_CUSTOMER_ORDER_LIST;
    tokens.put(field, null);
    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray arr = obj.getJSONArray(field);
      if (arr.length() > 0) {
        int element = rand.nextInt(arr.length());
        tokens.put(field, arr.getString(element));
      }
    }

    //array of arrays
    field = SOE_FIELD_CUSTOMER_LINKEDDEVICES;
    tokens.put(field, null);
    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray arr = obj.getJSONArray(field);
      if (arr.length() > 0) {
        int element = rand.nextInt(arr.length());
        JSONArray inarr = arr.getJSONArray(element);
        if (inarr.length() > 0) {
          int inelement = rand.nextInt(inarr.length());
          tokens.put(field, inarr.getString(inelement));
        }
      }
    }

    //array of objects
    field = SOE_FIELD_CUSTOMER_CHILDREN;
    tokens.put(field + SOE_SYSTEMFIELD_DELIMITER + SOE_FIELD_CUSTOMER_CHILDREN_OBJ_GENDER, null);
    tokens.put(field + SOE_SYSTEMFIELD_DELIMITER + SOE_FIELD_CUSTOMER_CHILDREN_OBJ_FNAME, null);
    tokens.put(field + SOE_SYSTEMFIELD_DELIMITER + SOE_FIELD_CUSTOMER_CHILDREN_OBJ_AGE, null);

    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray inarr = obj.getJSONArray(field);
      if (inarr.length() > 0) {
        int element = rand.nextInt(inarr.length());
        JSONObject inobj = inarr.getJSONObject(element);
        ArrayList<String> inobjStringFields = new ArrayList<>(Arrays.asList(
            SOE_FIELD_CUSTOMER_CHILDREN_OBJ_GENDER, SOE_FIELD_CUSTOMER_CHILDREN_OBJ_FNAME));

        for (String infield : inobjStringFields) {
          if (inobj.has(infield) && !inobj.isNull(infield)) {
            String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
            tokens.put(key, inobj.getString(infield));
          }
        }
        String infield = SOE_FIELD_CUSTOMER_CHILDREN_OBJ_AGE;
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
          tokens.put(key, String.valueOf(inobj.getInt(infield)));
        }
      }
    }

    //array of objects with array
    field = SOE_FIELD_CUSTOMER_VISITEDPLACES;
    tokens.put(field + SOE_SYSTEMFIELD_DELIMITER + SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY, null);
    tokens.put(field + SOE_SYSTEMFIELD_DELIMITER + SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES, null);

    if (obj.has(field) && !obj.isNull(field)) {
      JSONArray inarr = obj.getJSONArray(field);
      if (inarr.length()>0) {
        int element = rand.nextInt(inarr.length());
        JSONObject inobj = inarr.getJSONObject(element);
        String infield = SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_COUNTRY;
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
          tokens.put(key, inobj.getString(infield));
        }
        infield = SOE_FIELD_CUSTOMER_VISITEDPLACES_OBJ_CITIES;
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          JSONArray inarr2 = inobj.getJSONArray(infield);
          if (inarr2.length() >0) {
            int inelement2 = rand.nextInt(inarr2.length());
            String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
            tokens.put(key, inarr2.getString(inelement2));
          }
        }
      }
    }
  }

  private void tokenizeObjects(JSONObject obj, HashMap<String, String>  tokens) {

    //3-level nested objects
    String field = SOE_FIELD_CUSTOMER_ADDRESS;

    String l1Prefix = field + SOE_SYSTEMFIELD_DELIMITER;
    tokens.put(l1Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_CITY, null);
    tokens.put(l1Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP, null);
    tokens.put(l1Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY, null);
    tokens.put(l1Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_STREET, null);
    tokens.put(l1Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION, null);

    String l2Prefix = l1Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR + SOE_SYSTEMFIELD_DELIMITER;
    tokens.put(l2Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CITY, null);
    tokens.put(l2Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP, null);
    tokens.put(l2Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_STREET, null);
    tokens.put(l2Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_COUNTRY, null);


    String l3Prefix = l2Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER + SOE_SYSTEMFIELD_DELIMITER;
    tokens.put(l3Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_FNAME, null);
    tokens.put(l3Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_LNAME, null);
    tokens.put(l3Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_MNAME, null);
    tokens.put(l3Prefix + SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_PHONE, null);


    if (obj.has(field) && !obj.isNull(field)) {
      JSONObject inobj = obj.getJSONObject(field);

      ArrayList<String> inobjStringFields = new ArrayList<>(Arrays.asList(
          SOE_FIELD_CUSTOMER_ADDRESS_OBJ_CITY,
          SOE_FIELD_CUSTOMER_ADDRESS_OBJ_GEOREGION,
          SOE_FIELD_CUSTOMER_ADDRESS_OBJ_ZIP,
          SOE_FIELD_CUSTOMER_ADDRESS_OBJ_COUNTRY,
          SOE_FIELD_CUSTOMER_ADDRESS_OBJ_STREET));

      for (String infield : inobjStringFields) {
        if (inobj.has(infield) && !inobj.isNull(infield)) {
          String key = field + SOE_SYSTEMFIELD_DELIMITER + infield;
          tokens.put(key, inobj.getString(infield));
        }
      }

      String infield = SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR;
      if (inobj.has(infield) && !inobj.isNull(infield)) {
        JSONObject inobj2 = inobj.getJSONObject(infield);

        ArrayList<String> inobj2StringFields = new ArrayList<>(Arrays.asList(
            SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CITY,
            SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_ZIP,
            SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_STREET,
            SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_COUNTRY));

        for (String infield2 : inobj2StringFields) {
          if (inobj2.has(infield2) && !inobj2.isNull(infield2)) {
            String key = field + SOE_SYSTEMFIELD_DELIMITER + infield + SOE_SYSTEMFIELD_DELIMITER + infield2;
            tokens.put(key, inobj2.getString(infield2));
          }
        }

        String infield2 = SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER;
        if (inobj2.has(infield2) && !inobj2.isNull(infield2)) {
          JSONObject inobj3 = inobj2.getJSONObject(infield2);
          ArrayList<String> inobj3StringFields = new ArrayList<>(Arrays.asList(
              SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_FNAME,
              SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_LNAME,
              SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_MNAME,
              SOE_FIELD_CUSTOMER_ADDRESS_OBJ_PREVADDR_OBJ_CURRENTOWNER_OBJ_PHONE));

          for (String infield3 : inobj3StringFields) {
            if (inobj3.has(infield3) && !inobj3.isNull(infield3)) {
              String key = field + SOE_SYSTEMFIELD_DELIMITER +
                  infield + SOE_SYSTEMFIELD_DELIMITER + infield2 + SOE_SYSTEMFIELD_DELIMITER + infield3;
              tokens.put(key, inobj3.getString(infield3));
            }
          }
        }
      }
    }
  }

  private int getStoredCustomersCount() {
    if (storedDocsCountCustomer == 0) {
      storedDocsCountCustomer = Integer.parseInt(getVal(SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
          SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_CUSTOMER));
    }
    return storedDocsCountCustomer;
  }

  private int getTotalcustomersCount() {
    if (totalDocsCount == 0) {
      totalDocsCount = Integer.parseInt(getVal(SOE_DOCUMENT_PREFIX_CUSTOMER + SOE_SYSTEMFIELD_DELIMITER +
          SOE_SYSTEMFIELD_TOTALDOCS_COUNT));
    }
    return totalDocsCount;
  }

  private int getStoredOrdersCount(){
    if (storedDocsCountOrder == 0) {
      storedDocsCountOrder = Integer.parseInt(getVal(SOE_DOCUMENT_PREFIX_ORDER + SOE_SYSTEMFIELD_DELIMITER +
          SOE_SYSTEMFIELD_STORAGEDOCS_COUNT_ORDER));
    }
    return storedDocsCountOrder;
  }



  private int getNumberZipfianUnifrom() {
    if (zipfianGenerator == null) {
      zipfianGenerator = new ZipfianGenerator(1L, Long.valueOf(getStoredCustomersCount()-1).longValue());
    }
    return  zipfianGenerator.nextValue().intValue();
  }


  //getting latest docId shifted back on (max limit + max offest) to ensure the query returns expected amount of results
  private int getNumberZipfianLatests(int totalItems) {
    if (zipfianGenerator == null) {
      zipfianGenerator = new ZipfianGenerator(1L, Long.valueOf(getStoredCustomersCount()-1).longValue());
    }
    return  totalItems - zipfianGenerator.nextValue().intValue() - queryLimitMax - queryOffsetMax;
  }

  private int getNumberRandom(int limit) {
    return rand.nextInt(limit);
  }

}
