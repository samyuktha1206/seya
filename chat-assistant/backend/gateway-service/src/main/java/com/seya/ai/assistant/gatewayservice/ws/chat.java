package com.seya.ai.assistant.gatewayservice.ws;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;

import java.util.Map;

@Configuration
public class WebSocketConfig {

    @Bean
    public SimpleUrlHandlerMapping handlerMapping(WebSocketHandler chatWsHandler) {
        return new SimpleUrlHandlerMapping(Map.of("/ws/chat", chatWsHandler), 10);
    }
}

