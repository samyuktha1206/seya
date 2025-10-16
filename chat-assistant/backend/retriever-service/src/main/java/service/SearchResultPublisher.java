package service;

import config.KafkaTopicsProperties;
import model.events.SearchResultEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class SearchResultPublisher {
    private final KafkaTemplate<String, SearchResultEvent> kafka;
    private final KafkaTopicsProperties topics;

    public SearchResultPublisher(KafkaTemplate<String, SearchResultEvent> kafka, KafkaTopicsProperties topics) {
        this.kafka = kafka;
        this.topics = topics;
    }

    public void publish(String correlationId, SearchResultEvent event) {
        kafka.send(topics.searchResults(), correlationId, event);
    }

    public void flush() {
        kafka.flush();
    }
}
