package com.seya.ai.assistant.gatewayservice.ws;


import com.example.gateway.grpc.GrpcClientService;
import com.example.gateway.infra.SimpleRateLimiter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Component
public class ChatWebSocketHandler implements WebSocketHandler {

    private final GrpcClientService grpcClient;
    private final SimpleRateLimiter rateLimiter;
    private final ObjectMapper om = new ObjectMapper();

    public ChatWebSocketHandler(GrpcClientService grpcClient, SimpleRateLimiter rateLimiter) {
        this.grpcClient = grpcClient;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {

        // assign correlationId for this session
        String correlationId = UUID.randomUUID().toString();

        // send initial connected message with correlationId
        Mono<WebSocketMessage> welcome = Mono.fromSupplier(() ->
                session.textMessage("{\"type\":\"connected\",\"correlationId\":\"" + correlationId + "\"}")
        );

        // inbound messages (from browser)
        Flux<WebSocketMessage> inbound = session.receive();

        // We'll react to 'start' messages by calling gRPC stream and forwarding tokens.
        Flux<WebSocketMessage> outbound = inbound.flatMap(msg -> {
            String payload = msg.getPayloadAsText();
            try {
                JsonNode node = om.readTree(payload);
                String type = node.has("type") ? node.get("type").asText() : "start";
                if ("start".equals(type)) {
                    String query = node.has("query") ? node.get("query").asText() : "";
                    String userId = node.has("userId") ? node.get("userId").asText() : session.getId();

                    // rate limit per userId
                    if (!rateLimiter.tryConsume(userId)) {
                        return Mono.just(session.textMessage("{\"type\":\"error\",\"error\":\"rate_limited\"}"));
                    }

                    // call gRPC and stream tokens back
                    Flux<String> tokenFlux = grpcClient.streamResponse(correlationId, userId, query);

                    // convert tokens to websocket messages
                    return tokenFlux.map(token -> session.textMessage("{\"type\":\"token\",\"data\":" + om.writeValueAsString(token) + "}"))
                            .onErrorResume(ex -> Flux.just(session.textMessage("{\"type\":\"error\",\"error\":\"" + ex.getMessage() + "\"}")))
                            .concatWith(Mono.just(session.textMessage("{\"type\":\"complete\"}")));
                } else if ("cancel".equals(type)) {
                    // Cancel message — we could set up a cancellation mechanism correlated to the call.
                    // For simplicity, client cancel will close the WebSocket; alternatively you can track call references and cancel them.
                    return Mono.just(session.textMessage("{\"type\":\"cancel_ack\"}"));
                } else {
                    return Mono.just(session.textMessage("{\"type\":\"error\",\"error\":\"unknown_type\"}"));
                }
            } catch (Exception e) {
                return Mono.just(session.textMessage("{\"type\":\"error\",\"error\":\"invalid_json\"}"));
            }
        }).onErrorResume(e -> Mono.just(session.textMessage("{\"type\":\"error\",\"error\":\"" + e.getMessage() + "\"}")));

        // merge welcome message and outbound stream
        return session.send(welcome.concatWith(outbound)).and(Mono.never());
    }
}

