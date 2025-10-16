package model.events;

public record SearchResultEvent(
        String correlationId, //UUID per user request
        String query,
        String sourceDomain,
        String link,
        String title,
        String snippet,
        int rank, //1-based SERP position
        long fetchedAtMs // epoch mills when retriever fetched
) {
}
