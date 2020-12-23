package org.apache.iotdb;

import org.apache.iotdb.client.SingleClient;
import org.apache.iotdb.config.Config;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.commons.cli.*;

public class App {

  private static final Config config = Config.getConfig();
  static boolean needRun = true;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    SingleClient singleClient = new SingleClient();
    parseOptions(args);
    if(needRun) {
      singleClient.open(config.getHost(), config.getPort(), config.getUsername(), config.getPassword());
      singleClient.start();
    }
  }

  private static void parseOptions(String[] args){
    Options options=new Options();

    Option opt_help = new Option("help", "help", false, "print help message");
    opt_help.setRequired(false);
    options.addOption(opt_help);

    Option opt_host = new Option("h", "host", true, "host");
    opt_host.setRequired(false);
    options.addOption(opt_host);

    Option opt_port = new Option("p", "port", true, "port");
    opt_port.setRequired(false);
    options.addOption(opt_port);

    Option opt_username = new Option("u", "username", true, "username");
    opt_username.setRequired(false);
    options.addOption(opt_username);

    Option opt_password = new Option("pw", "password", true, "password");
    opt_password.setRequired(false);
    options.addOption(opt_password);

    Option opt_sensor_number = new Option("sn", "sensorNumber", true, "sensorNumber");
    opt_sensor_number.setRequired(false);
    options.addOption(opt_sensor_number);

    Option opt_group_number = new Option("sgn", "storageGroupNumber", true, "storageGroupNumber");
    opt_group_number.setRequired(false);
    options.addOption(opt_group_number);

    Option opt_device_number = new Option("dn", "deviceNumber", true, "deviceNumber");
    opt_device_number.setRequired(false);
    options.addOption(opt_device_number);

    Option opt_loop = new Option("l", "loop", true, "how many times correction test repeat");
    opt_loop.setRequired(false);
    options.addOption(opt_loop);

    Option opt_max_row_number = new Option("mrn", "maxRowNumber", true, "rowNumber");
    opt_max_row_number.setRequired(false);
    options.addOption(opt_max_row_number);

    Option insert_mode = new Option("im", "insertMode", true, "insert mode deletion, sequence, unsequence");
    insert_mode.setRequired(false);
    options.addOption(insert_mode);
    //用来打印帮助信息
    HelpFormatter hf=new HelpFormatter();
    hf.setWidth(110);

    CommandLine commandLine;
    CommandLineParser parser=new DefaultParser();

    try {
      commandLine = parser.parse(options,args);
      if(commandLine.hasOption("help")){
        hf.printHelp("testApp",options,true);
        needRun = false;
      }
      if(commandLine.hasOption("h")){
        config.setHost(commandLine.getOptionValue("h"));
      }
      if(commandLine.hasOption("p")) {
        config.setPort(Integer.parseInt(commandLine.getOptionValue("p")));
      }
      if(commandLine.hasOption("u")) {
        config.setUsername(commandLine.getOptionValue("u"));
      }
      if(commandLine.hasOption("pw")) {
        config.setPassword(commandLine.getOptionValue("pw"));
      }
      if(commandLine.hasOption("sn")) {
        config.setSensorNumber(Integer.parseInt(commandLine.getOptionValue("sn")));
      }
      if(commandLine.hasOption("sgn")) {
        config.setStorageGroupNumber(Integer.parseInt(commandLine.getOptionValue("sgn")));
      }
      if(commandLine.hasOption("dn")) {
        config.setDeviceNumber(Integer.parseInt(commandLine.getOptionValue("dn")));
      }
      if(commandLine.hasOption("l")) {
        config.setLoop(Integer.parseInt(commandLine.getOptionValue("l")));
      }
      if(commandLine.hasOption("mrn")) {
        config.setMaxRowNumber(Integer.parseInt(commandLine.getOptionValue("mrn")));
      }
      if(commandLine.hasOption("im")) {
        config.setINSERT_MODE("im");
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
}
