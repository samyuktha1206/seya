package com.seya.ai.assistant.gatewayservice.infra;


import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SimpleRateLimiter {

    private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    public boolean tryConsume(String key) {
        Bucket b = buckets.computeIfAbsent(key, k -> newBucket());
        return b.tryConsume(1);
    }

    private Bucket newBucket() {
        // example: 10 tokens per 10 seconds
        Refill refill = Refill.greedy(10, Duration.ofSeconds(10));
        Bandwidth limit = Bandwidth.classic(10, refill);
        return Bucket.builder().addLimit(limit).build();
    }
}

