package com.lakesoul.benchmark.zichan;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DomainLevelAssets {
    public static class PartitionInfoProcessFunction extends KeyedProcessFunction<String, Tuple7<String,String, String,Integer,Integer,Integer,Long>, Tuple6<String, Integer,Integer,Integer,Integer,Long>> {

        private MapState<String, NameSpaceCount> nameSpaceState;
        private transient ValueState<DomainAssets> domianState;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, NameSpaceCount> nameSpaceCountMapStateDescriptor =
                    new MapStateDescriptor<>("tableState",
                            String.class,
                            NameSpaceCount.class);


            ValueStateDescriptor<DomainAssets> domainAssetsValueStateDescriptor =
                    new ValueStateDescriptor<DomainAssets>("databaseState", DomainAssets.class);

            nameSpaceState = getRuntimeContext().getMapState(nameSpaceCountMapStateDescriptor);
            domianState = getRuntimeContext().getState(domainAssetsValueStateDescriptor);

        }

        @Override
        public void processElement(Tuple7<String,String, String,Integer,Integer,Integer,Long> input, Context ctx, Collector<Tuple6<String, Integer, Integer, Integer, Integer, Long>> out) throws Exception {
            String namespace = input.f0;
            String doamin = input.f2;
            String tabeOps = "create";
            String creator = input.f1;
            int tableCounts = input.f3;
            int partitionCounts = input.f4;
            int fileCounts = input.f5;
            long fileTotalSize = input.f6;
            System.out.println(input);
            //获取当前domain的统计信息
            DomainAssets currentDomainAssets = domianState.value();
            int currentNamespaceCounts;
            int currentTableCounts;
            int currentPartionCounts;
            int currentFileCounts;
            long currentFilesTotalSize;
            if (currentDomainAssets == null) {
                currentNamespaceCounts = 0;
                currentTableCounts = 0;
                currentPartionCounts = 0;
                currentFileCounts = 0;
                currentFilesTotalSize = 0;
            } else {
                currentNamespaceCounts = currentDomainAssets.namespaceCounts;
                currentTableCounts = currentDomainAssets.tableCounts;
                currentPartionCounts = currentDomainAssets.partitionCounts;
                currentFileCounts = currentDomainAssets.fileCounts;
                currentFilesTotalSize = currentDomainAssets.fileTotalSize;
            }
            if (!nameSpaceState.contains(namespace)){
                DomainAssets domainAssets = new DomainAssets(doamin,
                        currentNamespaceCounts+1,
                        currentTableCounts+tableCounts,
                        currentPartionCounts+partitionCounts,
                        currentFileCounts+fileCounts,
                        currentFilesTotalSize+fileTotalSize);
                domianState.update(domainAssets);

            } else {
                int oldTableCounts = nameSpaceState.get(namespace).tableCounts;
                int oldPartitionCounts = nameSpaceState.get(namespace).partitionCounts;
                int oldFileCounts = nameSpaceState.get(namespace).fileCounts;
                long oldFileTotalSize = nameSpaceState.get(namespace).fileTotalSize;
                DomainAssets domainAssets = new DomainAssets(doamin,
                        currentNamespaceCounts,
                        currentTableCounts+tableCounts-oldTableCounts,
                        currentPartionCounts+partitionCounts-oldPartitionCounts,
                        currentFileCounts+fileCounts-oldFileCounts,
                        currentFilesTotalSize+fileTotalSize-oldFileTotalSize
                        );
                domianState.update(domainAssets);
            }
            NameSpaceCount nameSpaceCount = new NameSpaceCount(namespace,doamin,tableCounts,fileCounts,partitionCounts,fileTotalSize);
            nameSpaceState.put(namespace,nameSpaceCount);
            out.collect(new Tuple6<>(doamin,domianState.value().namespaceCounts,domianState.value().tableCounts,domianState.value().partitionCounts,domianState.value().fileCounts,domianState.value().fileTotalSize));
        }
    }
}
