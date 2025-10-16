package config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties (prefix = "retrieval")
public record AllowListConfig(Resource allowlistFile, List<String> domains) {
}
