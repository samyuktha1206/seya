package config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public StringJsonMessageConverter jsonMessageConverter() {
        return new StringJsonMessageConverter();
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler(DeadLetterPublishingRecoverer dlpr) {
        return new DefaultErrorHandler(dlpr, new FixedBackOff(1000L, 5L));
    }

    @Bean
    public DeadLetterPublishingRecoverer dlpr(KafkaTemplate<Object, Object> template, KafkaTopicsProperties t) {
        return new DeadLetterPublishingRecoverer(template,
                ((consumerRecord, e) -> new TopicPartition(t.searchRequestsDlq(), consumerRecord.partition())));
    }

    @Bean
    public NewTopic searchRequests(KafkaTopicsProperties t) {
        return TopicBuilder.name(t.searchRequests()).partitions(6).replicas(1).build();
    }

    @Bean
    public NewTopic searchResults(KafkaTopicsProperties t) {
        return TopicBuilder.name(t.searchResults()).partitions(6).replicas(1).build();
    }
}
