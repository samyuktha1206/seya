package config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@Component
@ConfigurationProperties(prefix = "serper")
@Validated
public record SerperProperties (@NotBlank String baseUrl,
        @NotBlank String apiKey,
        @Min(200)int timeoutMs) {}
