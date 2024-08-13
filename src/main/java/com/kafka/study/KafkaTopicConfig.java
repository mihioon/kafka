package com.kafka.study;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {
//    @Value(value = "${kafka.bootstrapAddress}")
//    private String bootstrapAddress;
//
//    @Autowired
//    private KafkaTopicProperties kafkaTopicProperties;
//
//    @Bean
//    public KafkaAdmin kafkaAdmin() {
//        Map<String, Object> configs = new HashMap<>();
//        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
//        return new KafkaAdmin(configs);
//    }
//
//    /**
//     * @return
//     */
//    @Bean
//    public KafkaAdmin.NewTopics newTopics() {
//        List<NewTopic> topicList = new ArrayList<>();
//
//        // 동적으로 정의된 토픽 추가
//        kafkaTopicProperties.getTopics().forEach((key, config) -> {
//            topicList.add(TopicBuilder.name(config.getName())
//                    .partitions(config.getNumPartitions())
//                    .replicas(config.getReplicationFactor())
//                    .build());
//        });
//
//        return new KafkaAdmin.NewTopics(topicList.toArray(new NewTopic[0]));
//    }
}
