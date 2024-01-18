package com.lakesoul.benchmark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.types.Row;

public class WriteKafka {
    static String checkPointStorage = "file:///tmp/lakesoul/benchmark/kafka/ck";
    static String bootstrapServer = "localhost:9092";
    static String kafkaTopc = "your_kafka_topic";
    static String groupId = "your_consumer_group";
    static String warehousePath = "/tmp/lakesoul/benchmark/kafka/ware";
    static String tableName = "lakesoul_write_kafka_tbl";
    static int checkPointTime = 1000*60;
    static int parallelism = 4;

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.get("kafkaTopic")!=null){
            kafkaTopc = parameterTool.get("kafkaTopic");
        }
        if (parameterTool.get("bootstrapServer")!=null){
            bootstrapServer = parameterTool.get("bootstrapServer");
        }
        if (parameterTool.get("groupId")!=null){
            groupId = parameterTool.get("groupId");
        }
        if (parameterTool.get("warehousePath")!=null){
            warehousePath = parameterTool.get("warehousePath");
        }
        tableName = parameterTool.get("tableName") != null ? parameterTool.get("tableName") : tableName;
        checkPointTime = parameterTool.get("checkPointTime") !=null ? Integer.parseInt(parameterTool.get("checkPointTime" )) : checkPointTime;

        parallelism = parameterTool.get("parallelism") != null ? Integer.parseInt(parameterTool.get("parallelism")) : parallelism;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .enableCheckpointing(checkPointTime, CheckpointingMode.EXACTLY_ONCE);

        env.setParallelism(parallelism);

        env.getCheckpointConfig().setCheckpointStorage(checkPointStorage);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        KafkaSourceBuilder<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServer)
                .setTopics(kafkaTopc)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.timestamp(1703474520000L))
                .setValueOnlyDeserializer(new SimpleStringSchema());

        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource.build(), WatermarkStrategy.noWatermarks(),"kafka-data");

        SingleOutputStreamOperator<Row> targetDS = kafkaDS.map((MapFunction<String, String[]>) line -> {
                    return line.split(",");
                }).filter((FilterFunction<String[]>) array -> {
                    if (array.length == 9) return true;
                    else return false;
                }).map((MapFunction<String[], Row>) array -> Row.of( array[0], array[1], array[2], array[3], array[4], array[5], array[6], array[7], array[8]))
                .returns(
                        Types.ROW_NAMED(
                                new String[]{"client_ip", "domain", "time", "target_ip", "rcode", "query_type", "authority_record", "add_msg", "dns_ip"},
                                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING)
                );

        final Schema schema = Schema.newBuilder()
                .column("client_ip", DataTypes.VARCHAR(100))
                .column("domain", DataTypes.VARCHAR(10))
                .column("`time`", DataTypes.STRING())
                .column("target_ip", DataTypes.VARCHAR(20))
                .column("rcode", DataTypes.VARCHAR(20))
                .column("query_type", DataTypes.VARCHAR(20))
                .column("authority_record", DataTypes.VARCHAR(25))
                .column("add_msg", DataTypes.VARCHAR(25))
                .column("dns_ip", DataTypes.VARCHAR(20))
                .build();

        Table table = tEnvs.fromDataStream(targetDS);

        Catalog catalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakesoul",catalog);
        tEnvs.createTemporaryView("kafka_lakesoul",table);

        tEnvs.createTable("`lakesoul`.`default`."+tableName, TableDescriptor.forConnector("lakesoul")
                .schema(schema)
                .option("path",warehousePath+tableName)
                .build());
        tEnvs.executeSql("insert into `lakesoul`.`default`."+tableName+" select * from kafka_lakesoul");

        env.execute();
    }
}