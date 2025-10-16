package com.seya.ai.assistant.gatewayservice.grpc;

import com.example.gateway.generated.LLMProto;
import com.example.gateway.generated.LLMServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import javax.annotation.PreDestroy;
import java.time.Duration;

@Service
public class GrpcClientService {

    private final ManagedChannel channel;
    private final io.grpc.MethodDescriptor<LLMProto.QueryRequest, LLMProto.LLMResponse> method;

    public GrpcClientService(
            @Value("${llm.gateway.host}") String host,
            @Value("${llm.gateway.port}") int port) {

        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // in prod use TLS
                .keepAliveTime(30, java.util.concurrent.TimeUnit.SECONDS)
                .build();

        // Use the generated MethodDescriptor from the generated gRPC class
        this.method = LLMServiceGrpc.getStreamResponseMethod();
    }

    /**
     * Stream tokens from the LLM gateway for a single request.
     * Returns a Flux of JSON strings (or plain strings). Each element corresponds to one LLMResponse token.
     * Cancelling the Flux will cancel the gRPC call.
     */
    public Flux<String> streamResponse(String correlationId, String userId, String query) {
        LLMProto.QueryRequest req = LLMProto.QueryRequest.newBuilder()
                .setCorrelationId(correlationId)
                .setUserId(userId)
                .setQuery(query)
                .build();

        return Flux.create((FluxSink<String> sink) -> {
                    // Create a client call and invoke async server streaming
                    io.grpc.ClientCall<LLMProto.QueryRequest, LLMProto.LLMResponse> call =
                            channel.newCall(method, CallOptions.DEFAULT);

                    StreamObserver<LLMProto.LLMResponse> responseObserver = new StreamObserver<>() {
                        @Override
                        public void onNext(LLMProto.LLMResponse value) {
                            // forward token text as JSON string or raw token
                            String token = value.getToken();
                            // Wrap as JSON or simple string as you prefer
                            sink.next(token);
                        }

                        @Override
                        public void onError(Throwable t) {
                            sink.error(t);
                        }

                        @Override
                        public void onCompleted() {
                            sink.complete();
                        }
                    };

                    // Start the call
                    ClientCalls.asyncServerStreamingCall(call, req, new io.grpc.stub.StreamObserver<LLMProto.LLMResponse>() {
                        @Override
                        public void onNext(LLMProto.LLMResponse value) {
                            responseObserver.onNext(value);
                        }

                        @Override
                        public void onError(Throwable t) {
                            responseObserver.onError(t);
                        }

                        @Override
                        public void onCompleted() {
                            responseObserver.onCompleted();
                        }
                    });

                    // If the subscriber cancels the subscription, cancel the gRPC call
                    sink.onCancel(() -> {
                        call.cancel("client cancelled", null);
                    });

                }, FluxSink.OverflowStrategy.BUFFER)
                // choose a timeout guard to prevent runaway streams if you want
                .timeout(Duration.ofMinutes(5))
                ;
    }

    @PreDestroy
    public void shutdown() {
        if (!channel.isShutdown()) {
            channel.shutdownNow();
        }
    }
}

//This code uses channel.newCall(method, ...). The method is supplied by the generated LLMServiceGrpc.getStreamResponseMethod() method in generated code. The generated package and class names depend on your proto options — the pom/proto setup above will generate them under com.example.gateway.generated if you set options accordingly. Adjust imports if your package names differ.