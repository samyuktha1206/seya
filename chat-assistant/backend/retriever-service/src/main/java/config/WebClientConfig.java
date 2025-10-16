package config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientSsl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

@Configuration
public class WebClientConfig {
    @Bean
    WebClient serperWebClient(@Value("${serper.base_url}") String base_url,
                                 @Value("${serper.api_key}") String api_key,
                                 @Value("${serper.timeout-ms}") long timeoutMs) {
        return WebClient.builder()
                .baseUrl(base_url)
                .defaultHeader("X-API-KEY", api_key)
                .clientConnector(new ReactorClientHttpConnector(HttpClient.create().responseTimeout(Duration.ofMillis(timeoutMs))))
                .build();
    }
}
