package org.apache.iotdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * 300,000 sensors per device
 */
public class MassSensorTest {

    public static final int REPORT_COUNE = 50;
    static SessionPool sessionPool;

    /**
     * main function
     * @param args [iotdb_ip_address, iotdb_port, iotdb_username, iotdb_password, session_pool_size]
     */
    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            args = new String[]{"127.0.0.1", "6667", "root", "root", "50"};
        }
        sessionPool = new SessionPool(args[0], Integer.parseInt(args[1]), args[2], args[3], Integer.parseInt(args[4]));

        for (int i = 0; i < 6; i++) {
            new Thread(new WriteThread(i)).start();
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < 6; i++) {
            new Thread(new ReadLastThread(i)).start();
            new Thread(new ReadRawDataThread(i)).start();
            new Thread(new ReadGroupByThread(i)).start();
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new Thread(new WriteHistThread(1)).start();

    }

    static class WriteThread implements Runnable{
        int device;

        WriteThread(int device) {
            this.device = device;
        }

        long totalTime = 0;

        int count = 0;

        @Override
        public void run() {
            long time = 86400000;
            Random random = new Random();
            while (true) {
                count++;
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                long start = System.currentTimeMillis();


                time += 5000;
                String deviceId = "root.sg1.d1";
                List<String> measurements = new ArrayList<>();
                for (int i = 0; i < 50000; i++) {
                    measurements.add("s" + (i + device * 50000));
                }

                List<String> values = new ArrayList<>();
                for (int i = 0; i < 50000; i++) {
                    values.add(random.nextFloat()+"");
                }

                try {
                    sessionPool.insertRecord(deviceId, time, measurements, values);
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    e.printStackTrace();
                }

                totalTime += System.currentTimeMillis() - start;
                if(count % 50 == 0){
                    System.out.println(
                        Thread.currentThread().getName() + " write 50000 current points avg cost: " + (totalTime / count) + "ms");
                    count = 0;
                    totalTime = 0;
                }
            }
        }
    }

    // may not use here
    // write history data
    static class WriteHistThread implements Runnable{
        int device;

        WriteHistThread(int device) {
            this.device = device;
        }

        long totalTime = 0;

        int count = 0;

        @Override
        public void run() {
            long time = 0;
            Random random = new Random();
            while (true) {
                count++;
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                time += 5000;

                long start = System.currentTimeMillis();

                int index = random.nextInt(250000);

                List<MeasurementSchema> schemaList = new ArrayList<>();
                schemaList.add(new MeasurementSchema("s" + index, TSDataType.FLOAT, TSEncoding.RLE));
                schemaList.add(new MeasurementSchema("s" + (index+1), TSDataType.FLOAT, TSEncoding.RLE));
                schemaList.add(new MeasurementSchema("s" + (index+2), TSDataType.FLOAT, TSEncoding.RLE));
                schemaList.add(new MeasurementSchema("s" + (index+3), TSDataType.FLOAT, TSEncoding.RLE));
                schemaList.add(new MeasurementSchema("s" + (index+4), TSDataType.FLOAT, TSEncoding.RLE));
                schemaList.add(new MeasurementSchema("s" + (index+5), TSDataType.FLOAT, TSEncoding.RLE));
                schemaList.add(new MeasurementSchema("s" + (index+6), TSDataType.FLOAT, TSEncoding.RLE));
                schemaList.add(new MeasurementSchema("s" + (index+7), TSDataType.FLOAT, TSEncoding.RLE));
                schemaList.add(new MeasurementSchema("s" + (index+8), TSDataType.FLOAT, TSEncoding.RLE));
                schemaList.add(new MeasurementSchema("s" + (index+9), TSDataType.FLOAT, TSEncoding.RLE));


                Tablet tablet = new Tablet("root.sg1.d1", schemaList, 1000);

                long[] timestamps = tablet.timestamps;
                Object[] values = tablet.values;

                for (int num = 0; num < 50000; num++) {
                    int row = tablet.rowSize++;
                    timestamps[row] = time + num;
                    for (int i = 0; i < 10; i++) {
                        float[] sensor = (float[]) values[i];
                        sensor[row] = random.nextFloat();
                    }
                    if (tablet.rowSize == tablet.getMaxRowNumber()) {
                        try {
                            sessionPool.insertTablet(tablet, true);
                        } catch (IoTDBConnectionException | StatementExecutionException e) {
                            e.printStackTrace();
                        }
                        tablet.reset();
                    }
                }

                if (tablet.rowSize != 0) {
                    try {
                        sessionPool.insertTablet(tablet);
                    } catch (IoTDBConnectionException | StatementExecutionException e) {
                        e.printStackTrace();
                    }
                    tablet.reset();
                }

                totalTime += System.currentTimeMillis() - start;
                if(count % 50 == 0){
                    System.out.println(
                        Thread.currentThread().getName() + " write 500000 history points avg cost: " + (totalTime / count) + "ms");
                    count = 0;
                    totalTime = 0;
                }
            }
        }
    }

    static class ReadLastThread implements Runnable {
        int device;

        long totalTime = 0;

        int count = 0;

        ReadLastThread(int device) {
            this.device = device;
        }

        @Override
        public void run() {
            SessionDataSetWrapper dataSet = null;

            try {
                while (true) {
                    count++;
                    Thread.sleep(5000);
                    long start = System.currentTimeMillis();

                    StringBuilder builder = new StringBuilder("select last ");
                    for (int c = 50000*device; c < 50000*device + 49999; c++) {
                        builder.append("s").append(c).append(",");
                    }

                    builder.append("s" + ((device+1)*50000-1));
                    builder.append(" from root.sg1.d1");

                    dataSet = sessionPool.executeQueryStatement(builder.toString());
                    int a = 0;
                    while (dataSet.hasNext()) {
                        a++;
                        dataSet.next();
                    }

                    totalTime += System.currentTimeMillis() - start;
                    if(count % 50 == 0){
                        System.out.println(
                            Thread.currentThread().getName() + " last query avg cost: " + (totalTime / count) + "ms");
                        count = 0;
                        totalTime = 0;
                    }

                    sessionPool.closeResultSet(dataSet);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    static class ReadRawDataThread implements Runnable {
        int device;

        long totalTime = 0;

        int count = 0;

        ReadRawDataThread(int device) {
            this.device = device;
        }

        @Override
        public void run() {
            SessionDataSetWrapper dataSet = null;

            Random random = new Random();
            long time = 86400000;
            try {
                while (true) {
                    count++;
                    Thread.sleep(20000);
                    long start = System.currentTimeMillis();

                    StringBuilder builder = new StringBuilder("select ");

                    time += 5000;
                    builder.append("s" + random.nextInt(300000));
                    builder.append(" from root.sg1.d1 where time >= " + (time-86400000) + " and time <= " + time);

                    dataSet = sessionPool.executeQueryStatement(builder.toString());
                    int a = 0;
                    while (dataSet.hasNext()) {
                        a++;
                        dataSet.next();
                    }

                    totalTime += System.currentTimeMillis() - start;
                    if(count % REPORT_COUNE == 0){
                        System.out.println(
                            Thread.currentThread().getName() + " raw data query avg cost: " + (totalTime / count) + "ms");
                        count = 0;
                        totalTime = 0;
                    }

                    sessionPool.closeResultSet(dataSet);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    static class ReadGroupByThread implements Runnable {
        int device;

        long totalTime = 0;

        int count = 0;

        ReadGroupByThread(int device) {
            this.device = device;
        }

        @Override
        public void run() {
            SessionDataSetWrapper dataSet = null;

            Random random = new Random();
            long time = 86400000;
            try {
                while (true) {
                    count++;
                    Thread.sleep(20000);
                    long start = System.currentTimeMillis();

                    time += 5000;

                    StringBuilder builder = new StringBuilder("select ");

                    builder.append("last_value(s" + random.nextInt(300000) + ")");
                    builder.append(" from root.sg1.d1 group by ([" + (time-86400000) + "," + time + "), 5m) fill(int64[PREVIOUSUNTILLAST])");

                    dataSet = sessionPool.executeQueryStatement(builder.toString());

                    int a = 0;
                    while (dataSet.hasNext()) {
                        a++;
                        dataSet.next();
                    }

                    totalTime += System.currentTimeMillis() - start;
                    if(count % 50 == 0){
                        System.out.println(
                            Thread.currentThread().getName() + " down sampling query avg cost: " + (totalTime / count) + "ms");
                        count = 0;
                        totalTime = 0;
                    }

                    sessionPool.closeResultSet(dataSet);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}