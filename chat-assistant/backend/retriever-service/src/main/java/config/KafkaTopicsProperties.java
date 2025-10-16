package config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.topics")
public record KafkaTopicsProperties(String searchRequests, String searchResults, String searchRequestsDlq) {
}
