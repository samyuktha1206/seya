package config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@Component
@ConfigurationProperties (prefix = "retrieval")
@Validated
public record RetrievalProperties(@Min(1) @Max(100) int maxResult) {
}
