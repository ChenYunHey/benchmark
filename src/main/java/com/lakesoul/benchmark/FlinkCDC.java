package com.lakesoul.benchmark;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;

public class FlinkCDC {
    static String dataBase = "mysql_database";
    static String userName = "root";
    static String password = "123456";
    static String hostname = "localhost";
    static String port = "3306";
    static String timeZone = "Asia/Shanghai";
    static String sourceTableName = "mysql_source_table";
    static String targetTableName = "sink_lakesoul_table";
    static String warehousePath = "file:///tmp/lakesoul/data";
    static int hashBucketNum = 1;
    static int parallelism = 1;
    static int checkpointTime = 5000;
    static String chuckSize = "80960";
    static String fetchSize = "10240";
    static String poolSize = "50";
    static String checkpointPath = "file:///tmp/lakesoul/ck";

    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        hostname = parameterTool.get("hostname", hostname);
        port = parameterTool.get("port", port);
        dataBase = parameterTool.get("dbName", dataBase);
        userName = parameterTool.get("userName", userName);
        timeZone = parameterTool.get("timeZone", timeZone);
        chuckSize = parameterTool.get("chuckSize", chuckSize);
        fetchSize = parameterTool.get("fetchSize", fetchSize);
        poolSize = parameterTool.get("poolSize", poolSize);


        sourceTableName = parameterTool.get("sourceTableName", sourceTableName);
        targetTableName = parameterTool.get("targetTableName", targetTableName);
        warehousePath = parameterTool.get("warehousePath", warehousePath);

        hashBucketNum = parameterTool.getInt("bucketParallelism", hashBucketNum);
        parallelism = parameterTool.getInt("sourceParallelism", parallelism);
        password = parameterTool.get("password", password);
        checkpointTime = parameterTool.getInt("checkpointTime", checkpointTime);
        checkpointPath = parameterTool.get("checkpointPath", checkpointPath);

        Configuration configuration = new Configuration();
        configuration.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(checkpointTime, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
        env.setParallelism(hashBucketNum);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);

        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakesoul", lakesoulCatalog);
        String mysqlTable = String.format("create table `%s` (`client_ip` string, `domain` string, `time` string, `target_ip` string, `rcode` string," +
                        "`query_type` string, `authority_record` string, `add_msg` string, `dns_ip` string, PRIMARY KEY (`client_ip`, `domain`, `time`, `target_ip`) NOT ENFORCED) with (" +
                        "'connector' = 'mysql-cdc', " +
                        "'hostname' = '%s'," +
                        "'port' = '%s'," +
                        "'username' = '%s', " +
                        "'password' = '%s', " +
                        "'database-name' = '%s'," +
                        "'table-name' = '%s', " +
                        "'scan.startup.mode' = 'initial'," +
                        "'scan.incremental.snapshot.chunk.size' = '%s'," +
                        "'scan.snapshot.fetch.size' = '%s'," +
                        "'connection.pool.size' = '%s'," +
                        "'server-time-zone' = '%s'" +
                        ")",
                sourceTableName, hostname, port, userName, password, dataBase, sourceTableName, chuckSize, fetchSize, poolSize, timeZone);
        System.out.println(mysqlTable);
        tEnvs.executeSql(mysqlTable);

        String sql = "create table `lakesoul`.`default`.`%s` (" +
                "            `client_ip` varchar(100)," +
                "            domain varchar(10)," +
                "            `time` STRING," +
                "            target_ip VARCHAR(20)," +
                "            rcode varchar(20)," +
                "            query_type VARCHAR(20),å" +
                "            authority_record varchar(25)," +
                "            add_msg varchar(25)," +
                "            dns_ip varchar(20)," +
                "        PRIMARY KEY (client_ip,domain,`time`,target_ip) NOT ENFORCED" +
                "        ) " +
                "        WITH (" +
                "            'connector'='lakesoul'," +
                "            'hashBucketNum'='%s'," +
                "            'use_cdc'='true'," +
                "            'path'='%s');";
        String createTableSql = String.format(sql, targetTableName, hashBucketNum, warehousePath + "/" + targetTableName);
        tEnvs.executeSql(createTableSql);
        tEnvs.executeSql("insert into `lakesoul`.`default`." + targetTableName + " select * from " + sourceTableName);
    }
}