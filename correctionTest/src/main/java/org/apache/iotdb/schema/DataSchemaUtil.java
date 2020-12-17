package org.apache.iotdb.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class DataSchemaUtil {

  static List<MeasurementSchema> schemaList = new ArrayList<>();

  public static List<MeasurementSchema> getSchemaList(int sensorNumber) {
    if (schemaList.size() != sensorNumber) {
      for (int i = 1; i <= sensorNumber; i++) {
        schemaList.add(new MeasurementSchema("s" + i, TSDataType.INT64));
      }
    }
    return schemaList;
  }

  public static List<MeasurementSchema> getSingleSchemaList(int n) {
    return Collections.singletonList(new MeasurementSchema("s" + n, TSDataType.INT64));
  }
}
