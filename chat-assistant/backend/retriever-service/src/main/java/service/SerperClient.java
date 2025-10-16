package service;

import model.SerperResponse;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Map;

@Service
public class SerperClient {
    private final WebClient serper;

    public SerperClient(WebClient serperWebClient) {
        this.serper = serperWebClient;
    }

    public Mono<SerperResponse> search(String query, int maxResult) {
        return serper.post()
                .uri("/search")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("q", query, "num", maxResult))
                .retrieve()
                .bodyToMono(SerperResponse.class);
    }

}
