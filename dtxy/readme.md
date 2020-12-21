# 大唐先一测试负载模拟

## 目的
模拟大唐先一场景，发版前进行长测

## 负载描述
6个实时写入线程，每个线程每500ms写入50000个点

6个读last数据线程，每个线程5000ms读取50000个点的last数据

6个读原始数据线程，每个线程20000ms读取一个点的最近十天的数据

6个group by读线程，每个线程20000ms读取一个点的最近十天的数据的last_value聚合数据

1个写历史数据线程，每5000ms写10个时间序列，每一个时间序列写入5000个点，使用insert tablet实现


## 使用方法
```
java org.apache.iotdb.DTXYTest [iotdb_ip_address, iotdb_port, iotdb_username, iotdb_password, session_pool_size]

java org.apache.iotdb.DTXYTest 127.0.0.1 6667 root root 10
```

## 测试关注点
每一个负载下均有一个计时，该时间会随着机器性能而变化，一般来说：

实时写入线程应该在1秒以内完成: write 50000 current points avg cost: xxx

读last线程应该在1秒以内完成: last query avg cost: xxx

读原始数据线程应该在5秒内完成: raw data query avg cost: xxx

group by线程应该在5秒内完成: down sampling query avg cost: xxx

写历史数据线程应该在10秒内完成:write 500000 history points avg cost: xxx

