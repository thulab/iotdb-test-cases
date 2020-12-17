package org.apache.iotdb.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.iotdb.config.Config;
import org.apache.iotdb.config.Constants;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.schema.DataSchemaUtil;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleClient {
  private Session session;
  Config config = Config.getConfig();
  String randomDevice;
  String randomSensor;
  private final Random random = new Random();
  private int lastTimePoint;
  private boolean isCorrect = true;


  //this is a last point of time interval where this test choose to delete or insert unsequence data
  private int randomN = 1;

  private static final Logger logger = LoggerFactory.getLogger(SingleClient.class);

  public SingleClient() {

  }

  public void open(String url, int port, String username, String password)
      throws IoTDBConnectionException {
    session = new Session(url, port, username, password);
    session.open(false);
  }

  public void start() throws StatementExecutionException, IoTDBConnectionException {
    for(int i = 0; i < config.getLoop(); i++) {
      insert(i);
      query(i);
    }
  }

  private void insert(int count) throws StatementExecutionException, IoTDBConnectionException {
    for(int i = 1; i <= config.getStorageGroupNumber(); i++) {
      for(int j = 1; j <= config.getDeviceNumber(); j++) {
        String device = "root.sg" + i + ".d" + j;
        Tablet tablet = new Tablet(device, DataSchemaUtil.getSchemaList(config.getSensorNumber()), 1000);
        for(int k = 1 + count* config.getMaxRowNumber(); k <= (1 + count) * config.getMaxRowNumber() ; k++) {
          tablet.addTimestamp(tablet.rowSize, k);
          for(int m = 1; m <= config.getSensorNumber(); m++) {
            tablet.addValue("s" + m, tablet.rowSize, (long) k);
          }
          tablet.rowSize++;
          if(k % 1000 == 0) {
            session.insertTablet(tablet);
            tablet = new Tablet(device, DataSchemaUtil.getSchemaList(config.getSensorNumber()), 1000);
          }
        }
        session.insertTablet(tablet);
      }
    }
    switch (config.getINSERT_MODE()) {
      case Constants.SEQUENCE:
        break;
      case Constants.UNSEQUENCE:
        insertUnsequenceData(count);
        break;
      case Constants.DELETION:
        deleteData(count);
        break;
    }
  }

  private void query(int count) throws StatementExecutionException, IoTDBConnectionException {
    switch (config.getINSERT_MODE()) {
      case Constants.SEQUENCE:
        String sql = generateRawDataQuerySql(true, true,count);
        SessionDataSet dataSet = session.executeQueryStatement(sql);
        checkRawData(dataSet, sql);
        sql = generateRawDataQuerySql(true, false, count);
        dataSet = session.executeQueryStatement(sql);
        checkRawData(dataSet,sql);
        sql = generateRawDataQuerySql(false, true, count);
        dataSet = session.executeQueryStatement(sql);
        checkRawData(dataSet,sql);
        sql = generateCountAggregatedQuerySql(true, true, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData(1000, dataSet, sql);
        sql = generateCountAggregatedQuerySql(true, false, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData(1000, dataSet, sql);
        sql = generateCountAggregatedQuerySql(false, true, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData(1000, dataSet, sql);
        sql = generateLastValueAggregatedQuerySql(true, true, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData(lastTimePoint, dataSet, sql);
        sql = generateLastValueAggregatedQuerySql(true, false, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData(lastTimePoint, dataSet, sql);
        sql = generateLastValueAggregatedQuerySql(false, true, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData(lastTimePoint, dataSet, sql);
        sql = generateCountAggregatedQuerySqlAllTime(true, true, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData((1 + count) * config.getMaxRowNumber(), dataSet, sql);
        sql = generateCountAggregatedQuerySqlAllTime(true, false, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData((1 + count) * config.getMaxRowNumber(), dataSet, sql);
        sql = generateCountAggregatedQuerySqlAllTime(false, true, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData((1 + count) * config.getMaxRowNumber(), dataSet, sql);
        sql = generateLastValueAggregatedQuerySqlAllTime(true, true, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData((1 + count) * config.getMaxRowNumber(), dataSet, sql);
        sql = generateLastValueAggregatedQuerySqlAllTime(true, false, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData((1 + count) * config.getMaxRowNumber(), dataSet, sql);
        sql = generateLastValueAggregatedQuerySqlAllTime(false, true, count);
        dataSet = session.executeQueryStatement(sql);
        checkAggregatedData((1 + count) * config.getMaxRowNumber(), dataSet, sql);
        sql = generateCountGroupByQuerySql(true, true, count);
        checkAggregatedData(100, dataSet, sql);
        if(isCorrect) {
          logger.info("Loop " + count + " result is correct");
        } else {
          isCorrect = true;
        }
        break;
      case Constants.UNSEQUENCE:
        sql = generateRawDataQuerySql(true, true, count);
        dataSet = session.executeQueryStatement(sql);
        while(dataSet.hasNext()) {
          RowRecord record = dataSet.next();
          List<Field> fields = record.getFields();
          if(record.getTimestamp() >= randomN - 999 && record.getTimestamp() <= randomN) {
            for(Field field : fields) {
              if(record.getTimestamp() + 1 != field.getLongV()) {
                logger.error("Results of sql: " + sql + " isn't correct");
                isCorrect = false;
                break;
              }
            }
          } else {
            for(Field field : fields) {
              if(record.getTimestamp() != field.getLongV()) {
                logger.error("Results of sql: " + sql + " isn't correct");
                isCorrect = false;
                break;
              }
            }
          }
          if(!isCorrect) {
            break;
          }
        }
        if(isCorrect) {
          logger.info("Loop " + count + " result is correct");
        } else {
          isCorrect = true;
        }
        break;
      case Constants.DELETION:
        sql = generateRawDataQuerySql(true, true, count);
        dataSet = session.executeQueryStatement(sql);
        while(dataSet.hasNext()) {
          RowRecord record = dataSet.next();
          List<Field> fields = record.getFields();
          if(record.getTimestamp() >= randomN - 999 && record.getTimestamp() <= randomN) {
            logger.error("Results of sql: " + sql + " isn't correct");
            isCorrect = false;
            break;
          } else {
            for(Field field : fields) {
              if(record.getTimestamp() != field.getLongV()) {
                logger.error("Results of sql: " + sql + " isn't correct");
                isCorrect = false;
                break;
              }
            }
          }
          if(!isCorrect) {
            break;
          }
        }
        if(isCorrect) {
          logger.info("Loop " + count + " result is correct");
        } else {
          isCorrect = true;
        }
        break;
    }
  }

  private void checkRawData(SessionDataSet dataSet, String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    while(dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();
      for (int i = 0; i < fields.size(); i++) {
        if(fields.get(i).isNull()) {
          logger.error("Results of sql: " + sql + " isn't correct");
          logger.error(dataSet.getColumnNames().get(i + 1) + " at time " + record.getTimestamp() + " of expected result is " + record.getTimestamp() + ", but in fact null");
          isCorrect = false;
          break;
        } else if (record.getTimestamp() != fields.get(i).getLongV()) {
          logger.error("Results of sql: " + sql + " isn't correct");
          logger.error(dataSet.getColumnNames().get(i + 1) + " at time " + record.getTimestamp() + " of expected result is "  + record.getTimestamp() +  ", but in fact " + fields.get(i).getLongV());
          isCorrect = false;
          break;
        }
      }
      if(!isCorrect) {
        break;
      }
    }
  }

  private void checkAggregatedData(long expected, SessionDataSet dataSet, String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    while(dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();
      for (int i = 0; i < fields.size(); i++) {
        if(fields.get(i).isNull()) {
          logger.error("Results of sql: " + sql + " isn't correct");
          logger.error(dataSet.getColumnNames().get(i) + " of expected result is " + expected + ", but in fact null");
          isCorrect = false;
          break;
        } else if (fields.get(i).getLongV() != expected) {
          logger.error("Results of sql: " + sql + " isn't correct");
          logger.error(dataSet.getColumnNames().get(i) + " of expected result is "  + expected +  ", but in fact " + fields.get(i).getLongV());
          isCorrect = false;
          break;
        }
      }
      if(!isCorrect) {
        break;
      }
    }
  }


  private void deleteData(int count) throws StatementExecutionException, IoTDBConnectionException {
    randomN = random.nextInt(config.getMaxRowNumber()) + 1 + count * config.getMaxRowNumber();
    if(randomN - 1000 < count * config.getMaxRowNumber()) {
      randomN = count * config.getMaxRowNumber() + 1000;
    }
    int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
    int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
    int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
    randomDevice = "root.sg" + randomSgNumber + ".d" + randomDNumber;
    randomSensor = "s" + randomSNumber;
    List<String> paths = new ArrayList<>();
    paths.add(randomDevice + "." + randomSensor);
    session.deleteData(paths, randomN - 999, randomN);
  }

  private void insertUnsequenceData(int count) throws StatementExecutionException, IoTDBConnectionException {
    session.executeNonQueryStatement("flush");
    randomN = random.nextInt(config.getMaxRowNumber()) + 1 + count * config.getMaxRowNumber();
    if(randomN - 1000 < count * config.getMaxRowNumber()) {
      randomN = count * config.getMaxRowNumber() + 1000;
    }
    int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
    int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
    int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
    randomDevice = "root.sg" + randomSgNumber + ".d" + randomDNumber;
    randomSensor = "s" + randomSNumber;
    Tablet tablet = new Tablet(randomDevice, DataSchemaUtil.getSingleSchemaList(randomSNumber), 1000);
    for(int k = randomN - 999; k <= randomN; k++) {
      tablet.addTimestamp(tablet.rowSize, k);
      tablet.addValue(randomSensor, tablet.rowSize, (long) (k + 1));
      tablet.rowSize++;
    }
    session.insertTablet(tablet);
  }

  public String generateRawDataQuerySql(boolean isSingleDevice, boolean isSingleSensor, int count) {
    StringBuilder sql = new StringBuilder("select ");
    switch (config.getINSERT_MODE()) {
      case Constants.SEQUENCE:
        if (isSingleSensor && isSingleDevice) {
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          sql.append("s").append(randomSNumber).append(" from ");
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleDevice) {
          sql.append("s").append(1);
          for (int i = 2; i <= config.getSensorNumber(); i++) {
            sql.append(",").append("s").append(i);
          }
          sql.append(" from ");
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleSensor) {
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          sql.append("s").append(randomSNumber).append(" from ");
          String sg = "root." + "sg" + randomSgNumber;
          sql.append(sg).append(".d1");
          for (int i = 2; i <= config.getDeviceNumber(); i++) {
            sql.append(",").append(sg).append(".d").append(i);
          }
          sql.append(" ");
        }
        // pick a time point
        int point =
            random.nextInt(config.getMaxRowNumber()) + 1 + count * config.getMaxRowNumber();
        if (point - 1000 < count * config.getMaxRowNumber()) {
          point = count * config.getMaxRowNumber() + 1000;
        }
        sql.append(" where time >= ").append(point - 999).append(" and time <= ").append(point);
        break;
      case Constants.DELETION:
      case Constants.UNSEQUENCE:
        sql.append(randomSensor).append(" ");
        sql.append("from ").append(randomDevice);
        // this is a last point of time interval where this test choose to query data
        int pointN = random.nextInt(1000) + 1 + randomN - 1000;
        if(pointN - 1000 < count * config.getMaxRowNumber()) {
          pointN = count * config.getMaxRowNumber() + 1000;
        }
        sql.append(" where time >= ").append(pointN - 999).append(" and time <= ").append(pointN);
        break;
    }
    return sql.toString();
  }

  public String generateLastValueAggregatedQuerySql(boolean isSingleDevice, boolean isSingleSensor, int count) {
    StringBuilder sql = new StringBuilder("select ");
    switch (config.getINSERT_MODE()) {
      case Constants.SEQUENCE:
        if (isSingleSensor && isSingleDevice) {
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          sql.append("last_value(s").append(randomSNumber).append(")").append(" from ");
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleDevice) {
          sql.append("last_value(s").append(1).append(")");
          for (int i = 2; i <= config.getSensorNumber(); i++) {
            sql.append(",").append("last_value(s").append(i).append(")");
          }
          sql.append(" from ");
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleSensor) {
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          sql.append("last_value(s").append(randomSNumber).append(")").append(" from ");
          String sg = "root." + "sg" + randomSgNumber;
          sql.append(sg).append(".d1");
          for (int i = 2; i <= config.getDeviceNumber(); i++) {
            sql.append(",").append(sg).append(".d").append(i);
          }
          sql.append(" ");
        }
        // pick a time point
        lastTimePoint =
            random.nextInt(config.getMaxRowNumber()) + 1 + count * config.getMaxRowNumber();
        if (lastTimePoint - 1000 < count * config.getMaxRowNumber()) {
          lastTimePoint = count * config.getMaxRowNumber() + 1000;
        }
        sql.append(" where time >= ").append(lastTimePoint - 999).append(" and time <= ").append(lastTimePoint);
        break;
      case Constants.DELETION:
      case Constants.UNSEQUENCE:
        sql.append("last_value(").append(randomSensor).append(")").append(" ");
        sql.append("from ").append(randomDevice);
        // this is a last point of time interval where this test choose to query data
        int pointN = random.nextInt(1000) + 1 + randomN - 1000 + count * config.getMaxRowNumber();
        if(pointN - 1000 < count * config.getMaxRowNumber()) {
          pointN = count * config.getMaxRowNumber() + 1000;
        }
        sql.append(" where time >= ").append(pointN - 999).append(" and time <= ").append(pointN);
        break;
    }
    return sql.toString();
  }

  public String generateCountAggregatedQuerySql(boolean isSingleDevice, boolean isSingleSensor, int count) {
    StringBuilder sql = new StringBuilder("select ");
    switch (config.getINSERT_MODE()) {
      case Constants.SEQUENCE:
        if (isSingleSensor && isSingleDevice) {
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          sql.append("count(s").append(randomSNumber).append(")").append(" from ");
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleDevice) {
          sql.append("count(s").append(1).append(")");
          for (int i = 2; i <= config.getSensorNumber(); i++) {
            sql.append(",").append("count(s").append(i).append(")");
          }
          sql.append(" from ");
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleSensor) {
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          sql.append("count(s").append(randomSNumber).append(")").append(" from ");
          String sg = "root." + "sg" + randomSgNumber;
          sql.append(sg).append(".d1");
          for (int i = 2; i <= config.getDeviceNumber(); i++) {
            sql.append(",").append(sg).append(".d").append(i);
          }
          sql.append(" ");
        }
        // pick a time point
        int point =
            random.nextInt(config.getMaxRowNumber()) + 1 + count * config.getMaxRowNumber();
        if (point - 1000 < count * config.getMaxRowNumber()) {
          point = count * config.getMaxRowNumber() + 1000;
        }
        sql.append(" where time >= ").append(point - 999).append(" and time <= ").append(point);
        break;
      case Constants.DELETION:
      case Constants.UNSEQUENCE:
        sql.append("count(").append(randomSensor).append(")").append(" ");
        sql.append("from ").append(randomDevice);
        // this is a last point of time interval where this test choose to query data
        int pointN = random.nextInt(1000) + 1 + randomN - 1000 + count * config.getMaxRowNumber();
        if(pointN - 1000 < count * config.getMaxRowNumber()) {
          pointN = count * config.getMaxRowNumber() + 1000;
        }
        sql.append(" where time >= ").append(pointN - 999).append(" and time <= ").append(pointN);
        break;
    }
    return sql.toString();
  }

  public String generateCountAggregatedQuerySqlAllTime(boolean isSingleDevice, boolean isSingleSensor, int count) {
    StringBuilder sql = new StringBuilder("select ");
    switch (config.getINSERT_MODE()) {
      case Constants.SEQUENCE:
        if (isSingleSensor && isSingleDevice) {
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          sql.append("count(s").append(randomSNumber).append(")").append(" from ");
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleDevice) {
          sql.append("count(s").append(1).append(")");
          for (int i = 2; i <= config.getSensorNumber(); i++) {
            sql.append(",").append("count(s").append(i).append(")");
          }
          sql.append(" from ");
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleSensor) {
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          sql.append("count(s").append(randomSNumber).append(")").append(" from ");
          String sg = "root." + "sg" + randomSgNumber;
          sql.append(sg).append(".d1");
          for (int i = 2; i <= config.getDeviceNumber(); i++) {
            sql.append(",").append(sg).append(".d").append(i);
          }
        }
        break;
      case Constants.DELETION:
      case Constants.UNSEQUENCE:
        sql.append("count(").append(randomSensor).append(")").append(" ");
        sql.append("from ").append(randomDevice);
        break;
    }
    return sql.toString();
  }

  public String generateLastValueAggregatedQuerySqlAllTime(boolean isSingleDevice, boolean isSingleSensor, int count) {
    StringBuilder sql = new StringBuilder("select ");
    switch (config.getINSERT_MODE()) {
      case Constants.SEQUENCE:
        if (isSingleSensor && isSingleDevice) {
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          sql.append("last_value(s").append(randomSNumber).append(")").append(" from ");
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleDevice) {
          sql.append("last_value(s").append(1).append(")");
          for (int i = 2; i <= config.getSensorNumber(); i++) {
            sql.append(",").append("last_value(s").append(i).append(")");
          }
          sql.append(" from ");
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleSensor) {
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          sql.append("last_value(s").append(randomSNumber).append(")").append(" from ");
          String sg = "root." + "sg" + randomSgNumber;
          sql.append(sg).append(".d1");
          for (int i = 2; i <= config.getDeviceNumber(); i++) {
            sql.append(",").append(sg).append(".d").append(i);
          }
        }
        break;
      case Constants.DELETION:
      case Constants.UNSEQUENCE:
        sql.append("last_value(").append(randomSensor).append(")").append(" ");
        sql.append("from ").append(randomDevice);
        break;
    }
    return sql.toString();
  }

  public String generateLastValueGroupByQuerySql(boolean isSingleDevice, boolean isSingleSensor, int count) {
    StringBuilder sql = new StringBuilder("select ");
    switch (config.getINSERT_MODE()) {
      case Constants.SEQUENCE:
        if (isSingleSensor && isSingleDevice) {
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          sql.append("last_value(s").append(randomSNumber).append(")").append(" from ");
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleDevice) {
          sql.append("last_value(s").append(1).append(")");
          for (int i = 2; i <= config.getSensorNumber(); i++) {
            sql.append(",").append("last_value(s").append(i).append(")");
          }
          sql.append(" from ");
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleSensor) {
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          sql.append("last_value(s").append(randomSNumber).append(")").append(" from ");
          String sg = "root." + "sg" + randomSgNumber;
          sql.append(sg).append(".d1");
          for (int i = 2; i <= config.getDeviceNumber(); i++) {
            sql.append(",").append(sg).append(".d").append(i);
          }
          sql.append(" ");
        }
        // pick a time point
        int point =
            random.nextInt(config.getMaxRowNumber()) + 1 + count * config.getMaxRowNumber();
        if (point - 1000 < count * config.getMaxRowNumber()) {
          point = count * config.getMaxRowNumber() + 1000;
        }
        sql.append(" group by ((").append(point - 1000).append(",").append(point).append("]").append(",").append("100").append(")");
        break;
      case Constants.DELETION:
      case Constants.UNSEQUENCE:
        sql.append("last_value(").append(randomSensor).append(")").append(" ");
        sql.append("from ").append(randomDevice);
        // this is a last point of time interval where this test choose to query data
        int pointN = random.nextInt(1000) + 1 + randomN - 1000 + count * config.getMaxRowNumber();
        if(pointN - 1000 < count * config.getMaxRowNumber()) {
          pointN = count * config.getMaxRowNumber() + 1000;
        }
        sql.append(" group by ((").append(pointN - 1000).append(",").append(pointN).append("]").append(",").append("100").append(")");
        break;
    }
    return sql.toString();
  }

  public String generateCountGroupByQuerySql(boolean isSingleDevice, boolean isSingleSensor, int count) {
    StringBuilder sql = new StringBuilder("select ");
    switch (config.getINSERT_MODE()) {
      case Constants.SEQUENCE:
        if (isSingleSensor && isSingleDevice) {
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          sql.append("count(s").append(randomSNumber).append(")").append(" from ");
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleDevice) {
          sql.append("count(s").append(1).append(")");
          for (int i = 2; i <= config.getSensorNumber(); i++) {
            sql.append(",").append("last_value(s").append(i).append(")");
          }
          sql.append(" from ");
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomDNumber = random.nextInt(config.getDeviceNumber()) + 1;
          sql.append("root.").append("sg").append(randomSgNumber).append(".d")
              .append(randomDNumber);
        } else if (isSingleSensor) {
          int randomSgNumber = random.nextInt(config.getStorageGroupNumber()) + 1;
          int randomSNumber = random.nextInt(config.getSensorNumber()) + 1;
          sql.append("count(s").append(randomSNumber).append(")").append(" from ");
          String sg = "root." + "sg" + randomSgNumber;
          sql.append(sg).append(".d1");
          for (int i = 2; i <= config.getDeviceNumber(); i++) {
            sql.append(",").append(sg).append(".d").append(i);
          }
          sql.append(" ");
        }
        // pick a time point
        int point =
            random.nextInt(config.getMaxRowNumber()) + 1 + count * config.getMaxRowNumber();
        if (point - 1000 < count * config.getMaxRowNumber()) {
          point = count * config.getMaxRowNumber() + 1000;
        }
        sql.append(" group by ((").append(point - 1000).append(",").append(point).append("]").append(",").append("100").append(")");
        break;
      case Constants.DELETION:
      case Constants.UNSEQUENCE:
        sql.append("count(").append(randomSensor).append(")").append(" ");
        sql.append("from ").append(randomDevice);
        // this is a last point of time interval where this test choose to query data
        int pointN = random.nextInt(1000) + 1 + randomN - 1000 + count * config.getMaxRowNumber();
        if(pointN - 1000 < count * config.getMaxRowNumber()) {
          pointN = count * config.getMaxRowNumber() + 1000;
        }
        sql.append(" group by ((").append(pointN - 1000).append(",").append(pointN).append("]").append(",").append("100").append(")");
        break;
    }
    return sql.toString();
  }
}
