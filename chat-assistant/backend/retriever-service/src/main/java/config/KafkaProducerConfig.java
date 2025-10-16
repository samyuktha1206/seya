package config;

import model.events.SearchResultEvent;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

//@Configuaration used to tell spring that this class has bean definitions. Runs these classes at statrup and registeres the beans and creates them. But will inject only lazily when called.
@Configuration
public class KafkaProducerConfig {

    //@Bean tells spring to register the return value of this method as a bean
    //ProducerFactory<>() -> Interface used for creating Kafka Producers. DefaultKafkaProducerFactory is the implementation of this interface.
    //KafkaProperties class reads Kafka Properties from applications.yml
    //props.buildProducerProperties() creates a Map<String, Object> of the kafka properties mentioned in appplication.yml to be fed into ProducerFactory to help build producers to our specification.
    @Bean
    public ProducerFactory<String, SearchResultEvent> searchResultEventProducerFactory(KafkaProperties props) {
        return new DefaultKafkaProducerFactory<>(props.buildProducerProperties());
    }

    //@KafkaTemplate is a wrapper over Producer which helps to send payloads to kafka Topics. Sending directly from Producers is slightly complicated. Read more about it!
    @Bean
    public KafkaTemplate<String, SearchResultEvent> searchResultEventKafkaTemplate(ProducerFactory<String, SearchResultEvent> pf) {
        return new KafkaTemplate<>(pf);
    }
}
