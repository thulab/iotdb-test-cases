package org.apache.iotdb.client;

import org.apache.iotdb.config.Config;
import org.junit.Test;

public class SingleClientTest {

  Config config = Config.getConfig();

  @Test
  public void testGenerateSql() {
    SingleClient singleClient = new SingleClient();
    System.out.println(singleClient.generateRawDataQuerySql(true, true, 0));
    System.out.println(singleClient.generateRawDataQuerySql(true, false, 0));
    System.out.println(singleClient.generateRawDataQuerySql(false, true, 0));
    System.out.println(singleClient.generateCountAggregatedQuerySql(true, true, 0));
    System.out.println(singleClient.generateCountAggregatedQuerySql(false, true, 0));
    System.out.println(singleClient.generateCountAggregatedQuerySql(true, false, 0));
  }

}
