package service;

import model.RetrieverRequest;
import model.events.SearchResultEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;

@Component
public class SearchRequestListener {
    private final RetrievalService retrievalService;
    private final SearchResultPublisher searchResultPublisher;

    public SearchRequestListener(RetrievalService retrievalService, SearchResultPublisher searchResultPublisher) {
        this.retrievalService = retrievalService;
        this.searchResultPublisher = searchResultPublisher;
    }

    @KafkaListener (topics = "${app.topics.search-requests}")
    public void onMessage(RetrieverRequest req, @Header(KafkaHeaders.RECEIVED_KEY) String key, Acknowledgment ack) {
        if(req == null || req.query().isEmpty()) throw new IllegalArgumentException("query is required");

        final String correlationId = (req.correlationId() != null && !req.correlationId().isBlank())?req.correlationId() : ((key !=null && !key.isBlank())?key : java.util.UUID.randomUUID().toString());

        try{
            var hits = retrievalService.retrieve(req.query(), req.maxResults()).blockOptional().orElse(List.of());
            final long now = System.currentTimeMillis();
            int rank = 0;
            for(var h : hits) {
                var domain = extractDomain(h.url());
                var event = new SearchResultEvent(
                        correlationId,
                        req.query(),
                        domain,
                        h.url(),
                        h.title(),
                        h.snippet(),
                        ++rank,
                        now
                );
                searchResultPublisher.publish(correlationId, event);
            }
            searchResultPublisher.flush();
            ack.acknowledge();
        } catch (Exception e) {
            throw e;
        }
    }

    private String extractDomain(String url) {
        try {
           String host = new URI(url).getHost();
           return host==null?"":host.replaceFirst("^www:\\.","");
        } catch (Exception e){
            return "";
        }
    }
}
