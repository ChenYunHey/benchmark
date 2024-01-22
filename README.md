<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->


1. clone this projection, in the base direction package jar with: mvn package; and copy benchmarkTest-1.0-SNAPSHOT.jar to $FLINK_HOME
2. copy lakesoul-flink-2.5.0-flink-1.17-SNAPSHOT.jar to $FLINK_HOME directory
3. start flink cluster


run flink sql cdc
./bin/flink run -c com.lakesoul.benchmark.FlinkCDC \ 
    -C file://$FLINK_HOME/lakesoul-flink-2.5.0-flink-1.17-SNAPSHOT.jar $FLINK_HOME/benchmarkTest-1.0-SNAPSHOT.jar \
    --dbName benchmark \
    --userName root \
    --password 123456 \
    --bucketParallelism 1 \
    --sourceParallelism 1 \
    --sourceTableName lakesoul_test_mysql_table  \
    --targetTableName lakesoul_test_mysql_table \
    --warehousePath file:///tmp/lakesoul/flink_sql_cdc/data \
    --checkpointPath file:///tmp/lakesoul/flink_sql_cdc/ck \
    --checkpointTime 60000 \
    --hostname 127.0.0.1 \
    --port 3306 \
    --chuckSize 80960 \
    --fetchSize 10240





