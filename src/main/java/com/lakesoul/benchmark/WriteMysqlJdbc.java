package com.lakesoul.benchmark;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;

public class WriteMysqlJdbc {
    static String dataBase = "jdbccdc";
    static String userName = "root";
    static String  password = "123456";
    static String url = "jdbc:mysql://localhost:3306/";
    static String sourceTableName = "lakesoul_test_mysql_table";
    static String targetTableName = "lakesoul_mysql1";
    static String warehousePath = "/tmp/lakesoul/mysql2/";
    static int hashBucketNum = 4;
    static int parallelism = 4;
    static int checkpointTime = 3000;
    public static void main(String[] args) {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        dataBase = parameterTool.get("dbName",dataBase);
        userName = parameterTool.get("userName",userName);
        url = parameterTool.get("url","jdbc:mysql://localhost:3306/");
        sourceTableName = parameterTool.get("sourceTableName","lakesoul_test_mysql_table");
        targetTableName = parameterTool.get("targetTableName","lakesoul_test_mysql_table");
        warehousePath = parameterTool.get("warehousePath", "/tmp/lakesoul/mysql2/");
        hashBucketNum = parameterTool.getInt("bucketParallelism",4);
        parallelism = parameterTool.getInt("sourceParallelism",4);
        password = parameterTool.get("password",password);
        checkpointTime = parameterTool.getInt("checkpointTime",3000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .enableCheckpointing(checkpointTime, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(parallelism);
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);

        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakesoul",lakesoulCatalog);
        String mysqlCtalog = String.format("create catalog mysql_catalog with('type'='jdbc','default-database'='%s','username'='%s','password'='%s','base-url'='%s')",
                dataBase,userName,password,url);
        tEnvs.executeSql(mysqlCtalog);

        String sql = "create table `lakesoul`.`default`.`%s` (" +
                "            `client_ip` varchar(100)," +
                "            domain varchar(10)," +
                "            `time` STRING," +
                "            target_ip VARCHAR(20)," +
                "            " +
                "            rcode varchar(20),"+
                "            query_type VARCHAR(20),"+
                "            authority_record varchar(25),"+
                "            add_msg varchar(25)," +
                "            dns_ip varchar(20)," +
                "        PRIMARY KEY (client_ip,domain,`time`,target_ip) NOT ENFORCED" +
                "        ) " +
                "        WITH (" +
                "            'connector'='lakesoul'," +
                "            'hashBucketNum'='%s'," +
                "            'use_cdc'='true'," +
                "            'path'='%s');";
        String createTableSql = String.format(sql,targetTableName,hashBucketNum,warehousePath+targetTableName);
        tEnvs.executeSql(createTableSql);
        tEnvs.executeSql("insert into `lakesoul`.`default`."+targetTableName+" select * from mysql_catalog."+dataBase+"."+sourceTableName);
    }
}