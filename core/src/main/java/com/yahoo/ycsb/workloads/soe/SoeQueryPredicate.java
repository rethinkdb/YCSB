package com.yahoo.ycsb.workloads.soe;

/**
 * Created by oleksandr.gyryk on 3/20/17.
 */
public class SoeQueryPredicate {

  public static final String SOE_PREDICATE_TYPE_STRING = "string";
  public static final String SOE_PREDICATE_TYPE_INTEGER = "int";
  public static final String SOE_PREDICATE_TYPE_BOOLEAN = "bool";


  private String name;
  private String valueA;
  private String valueB;
  private String docid;
  private String operation;
  private String relation;
  private String type = SOE_PREDICATE_TYPE_STRING;


  private SoeQueryPredicate nestedPredicateA = null;
  private SoeQueryPredicate nestedPredicateB = null;
  private SoeQueryPredicate nestedPredicateC = null;
  private SoeQueryPredicate nestedPredicateD = null;


  public void setName(String name) {
    this.name = name;
  }

  public void setValueA(String valueA) {
    this.valueA = valueA;
  }

  public void setValueB(String valueB) {
    this.valueB = valueB;
  }

  public void setDocid(String docid) {
    this.docid = docid;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public void setRelation(String relation) {
    this.relation = relation;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setNestedPredicateA(SoeQueryPredicate nestedPredicateA) {
    this.nestedPredicateA = nestedPredicateA;
  }

  public void setNestedPredicateB(SoeQueryPredicate nestedPredicateB) {
    this.nestedPredicateB = nestedPredicateB;
  }

  public void setNestedPredicateC(SoeQueryPredicate nestedPredicateC) {
    this.nestedPredicateC = nestedPredicateC;
  }

  public void setNestedPredicateD(SoeQueryPredicate nestedPredicateD) {
    this.nestedPredicateD = nestedPredicateD;
  }



  public String getName() {
    return name;
  }

  public String getValueA() {
    return valueA;
  }

  public String getValueB() {
    return valueB;
  }

  public String getDocid() {
    return docid;
  }

  public String getOperation() {
    return operation;
  }

  public String getRelation() {
    return relation;
  }

  public String getType() {
    return type;
  }

  public SoeQueryPredicate getNestedPredicateA() {
    return nestedPredicateA;
  }

  public SoeQueryPredicate getNestedPredicateB() {
    return nestedPredicateB;
  }

  public SoeQueryPredicate getNestedPredicateC() {
    return nestedPredicateC;
  }

  public SoeQueryPredicate getNestedPredicateD() {
    return nestedPredicateD;
  }
}
