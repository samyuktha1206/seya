package model;

import java.util.List;

public record RetrieverResponse(String correlationId, List<LinkResult> link) {
}
