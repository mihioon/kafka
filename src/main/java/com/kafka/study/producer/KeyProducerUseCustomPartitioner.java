package com.kafka.study.producer;

import com.kafka.study.partitioner.CustomPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class KeyProducerUseCustomPartitioner {
    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static String TOPIC_NAME = "custom_topic";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        /**
         * 전달할 메시지 값을 생성
         */
        String messageValue = "[커스텀토픽]";

        Map<String, String> records = new HashMap<>();
        records.put("partition_1", "m1 " + messageValue);
        records.put("partition_2", "m2 " + messageValue);
        records.put("partition_3", "m3 " + messageValue);
        records.put(null, "m5 " + messageValue);

        ProducerRecord<String, String> record;

        for (String messageKey : records.keySet()) {
            log.info("messageKey=============={}", messageKey);
            record = new ProducerRecord<>(TOPIC_NAME, messageKey, records.get(messageKey));
            producer.send(record);
            log.info("{}", record);
        }

        /**
         * 애플리케이션을 안전하게 종료
         */
        producer.flush();
        producer.close();
    }
}
