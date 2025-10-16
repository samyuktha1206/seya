package model;

import jakarta.validation.constraints.NotBlank;

public record RetrieverRequest(@NotBlank String query, Integer maxResults, String correlationId) {
}
