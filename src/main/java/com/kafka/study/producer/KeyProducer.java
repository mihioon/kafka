package com.kafka.study.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

@Slf4j
public class KeyProducer {
    private final static String BOOTSTRAP_SERVER = "localhost:9092"; // @Value(value = "${kafka.bootstrapAddress}")
    private final static String TOPIC_NAME = "custom_topic";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER); //  프로듀서의 인스턴스에 사용할 '필수 옵션' 설정

        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        /**
         * 전달할 메시지 값을 생성
         */
        String messageKey = "sampleKey";
        String messageValue = "[커스텀토픽]";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue);
        producer.send(record);

        log.info("{}", record);

        /**
         * 애플리케이션을 안전하게 종료
         */
        producer.flush();
        producer.close();
    }
}
