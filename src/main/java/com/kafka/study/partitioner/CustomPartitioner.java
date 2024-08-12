package com.kafka.study.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

@Slf4j
public class CustomPartitioner implements Partitioner {
    /**
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        /**
         * 레코드에 메시지 키를 지정하지 않은 경우 -> 비정상적인 데이터로 간주하고 InvalidRecordException 발생 처리.
         */
        if (keyBytes == null) {
            log.info("널 처리 로직이 확인되다니 럭키비키잔앙🍀");
            throw new InvalidRecordException("Message key가 비어있습니다.");
        }

        /**
         * key에 따라 파티션 지정
         */
        String keyName = (String) key;
        if (keyName.equals("partition_1")) {
            return 0;
        } else if (keyName.equals("partition_2")) {
            return 1;
        } else if (keyName.equals("partition_3")) {
            return 2;
        }

        /**
         * 메시지 키가 존재하지만 지정된 key가 아닌 경우
         * 해시값을 지정하여 특정 파티션에 매칭되도록 한다.
         */
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        // Kafka에서 파티션을 결정할 때, 메시지의 키에 대해 해시를 계산하고, 그 해시 값을 기반으로 파티션을 결정
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    /**
     *
     */
    @Override
    public void close() {

    }

    /**
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
