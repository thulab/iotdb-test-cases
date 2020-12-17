### before using it, you have to compile it

```
mvn clean package
```

### run this test in target

```
java - jar correctionTest-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### change some params

you can get more details by running

```
java - jar correctionTest-1.0-SNAPSHOT-jar-with-dependencies.jar -help
```

for example, you can change host, port, etc

```
java -jar jar correctionTest-1.0-SNAPSHOT-jar-with-dependencies.jar -h localhost -p 6667
```

in default mode

```
INSERT_MODE = Constants.SEQUENCE;
Loop = 2;
sensorNumber = 10;
storageGroupNumber = 10;
deviceNumber = 10;
maxRowNumber = 100000;
host = "127.0.0.1";
port = 6667;
username = "root";
password = "root";
```
