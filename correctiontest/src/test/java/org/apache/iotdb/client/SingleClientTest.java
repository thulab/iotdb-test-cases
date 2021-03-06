package org.apache.iotdb.client;

import org.junit.Test;

public class SingleClientTest {

  @Test
  public void testGenerateSql() {
    SingleClient singleClient = new SingleClient();
    System.out.println(singleClient.generateRawDataQuerySql(true, true, 0));
    System.out.println(singleClient.generateRawDataQuerySql(true, false, 0));
    System.out.println(singleClient.generateRawDataQuerySql(false, true, 0));
    System.out.println(singleClient.generateCountAggregatedQuerySql(true, true, 0));
    System.out.println(singleClient.generateCountAggregatedQuerySql(false, true, 0));
    System.out.println(singleClient.generateCountAggregatedQuerySql(true, false, 0));
    System.out.println(singleClient.generateCountGroupByQuerySql(true, true, 0));
  }

}
