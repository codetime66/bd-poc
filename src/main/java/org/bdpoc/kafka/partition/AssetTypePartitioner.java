package org.bdpoc.kafka.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AssetTypePartitioner implements Partitioner {
    private static Map<String,Integer> assetTypeToPartitionMap;

    //called at the start.
    public void configure(Map<String, ?> configs) {
        System.out.println("Inside AssetTypePartitioner.configure " + configs);
        assetTypeToPartitionMap = new HashMap<String, Integer>();
        for(Map.Entry<String,?> entry: configs.entrySet()){
            if(entry.getKey().startsWith("partitions.")){
                String keyName = entry.getKey();
                String value = (String)entry.getValue();
                System.out.println( keyName.substring(11));
                int paritionId = Integer.parseInt(keyName.substring(11));
                assetTypeToPartitionMap.put(value,paritionId);
            }
        }
    }

    //This method will get called once for each message
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
                         Cluster cluster) {
        List partitions = cluster.availablePartitionsForTopic(topic);
        String valueStr = (String)value;
        String assetTypeName = ((String) value).split(":")[0];
        if(assetTypeToPartitionMap.containsKey(assetTypeName)){
            //If the assetType is mapped to particular partition return it
            return assetTypeToPartitionMap.get(assetTypeName);
        }else {
            //If no assetType is mapped to particular partition distribute between remaining partitions
            int noOfPartitions = cluster.topics().size();
            return  value.hashCode()%noOfPartitions + assetTypeToPartitionMap.size() ;
        }
    }

    //called at the end.
    public void close() {}
}
