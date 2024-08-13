package com.kafka.study;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "spring.kafka.topics")
public class KafkaTopicProperties {
    private Map<String, TopicAlarm> topics;

    @Getter
    @Setter
    public static class TopicAlarm {
        private String name;
        private int numPartitions;
        private int replicationFactor;
    }
}
