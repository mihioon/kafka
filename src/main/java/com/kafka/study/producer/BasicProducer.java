package com.kafka.study.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

@Slf4j
public class BasicProducer {
    /**
     * 프로듀서 애플리케이션에서는 데이터를 전달할 브로커의 호스트 ip와 토픽명을 알고 있어야 한다.
     * [참고] 브로커에 해당 토픽명이 없는 경우에는 기본 설정에 따라서는 토픽을 생성하고 데이터를 넣어준다.
     */
    private final static String BOOTSTRAP_SERVER = "localhost:9092"; // @Value(value = "${kafka.bootstrapAddress}")
    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER); //  프로듀서의 인스턴스에 사용할 '필수 옵션' 설정

        /**
         * 메시지 키, 값을 직렬화 하기 위해 StringSerializer를 사용한다.
         * StringSerializer는 String을 직렬화하는 카프카의 라이브러리이다.
         * (org.apache.kafka.common.serialization)
         */
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * 프로듀서 인스턴스를 생성하며, 위에서 설정한 설정을 파라미터로 사용한다.
         */
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        /**
         * 전달할 메시지 값을 생성한다.
         * (여기서는 애플리케이션 실행 시점의 날짜와 시간을 조합하여서 메시지 값으로 생성한다.)
         */
        Date todayDate = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String messageValue = "m1 [" + dateFormat.format(todayDate) + "]";

        /**
         * 레코드를 생성하고 전달한다.
         * 이때, 레코드를 전달할 토픽과 레코드의 메시지 값을 지정한다.
         */
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        producer.send(record);

        log.info("{}", record);

        /**
         * 애플리케이션을 안전하게 종료한다.
         */
        producer.flush();
        producer.close();
    }
}
