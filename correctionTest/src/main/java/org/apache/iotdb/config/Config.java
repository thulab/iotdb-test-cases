package org.apache.iotdb.config;

public class Config {
  private String INSERT_MODE = Constants.SEQUENCE;
  private int Loop = 2;
  private int sensorNumber = 10;
  private int storageGroupNumber = 10;
  private int deviceNumber = 10;
  private int maxRowNumber = 100000;
  private String host = "127.0.0.1";
  private int port = 6667;
  private String username = "root";
  private String password = "root";

  public void setHost(String host) {
    this.host = host;
  }

  public String getHost() {
    return host;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getPort() {
    return port;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getPassword() {
    return password;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getUsername() {
    return username;
  }

  public int getSensorNumber() {
    return sensorNumber;
  }

  public void setSensorNumber(int sensorNumber) {
    this.sensorNumber = sensorNumber;
  }

  public int getStorageGroupNumber() {
    return storageGroupNumber;
  }

  public void setStorageGroupNumber(int storageGroupNumber) {
    this.storageGroupNumber = storageGroupNumber;
  }

  public int getDeviceNumber() {
    return deviceNumber;
  }

  public void setDeviceNumber(int deviceNumber) {
    this.deviceNumber = deviceNumber;
  }

  public void setMaxRowNumber(int maxRowNumber) {
    this.maxRowNumber = maxRowNumber;
  }

  public int getMaxRowNumber() {
    return maxRowNumber;
  }

  public int getLoop() {
    return Loop;
  }

  public void setLoop(int loop) {
    Loop = loop;
  }

  public String getINSERT_MODE() {
    return INSERT_MODE;
  }

  public void setINSERT_MODE(String INSERT_MODE) {
    this.INSERT_MODE = INSERT_MODE;
  }

  private static class ConfigHolder{

    private static final Config instance = new Config();
  }

  public static Config getConfig() {
    return ConfigHolder.instance;
  }
}
