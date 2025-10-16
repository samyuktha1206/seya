package service;

import config.AllowListConfig;
import model.LinkResult;
import model.SerperResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class RetrievalService {
    private final SerperClient serperClient;
    private final AllowListConfig allowlist;
    private final int maxResult;

    public RetrievalService(SerperClient serperClient, AllowListConfig allowlist, @Value("${retrieval.max-result}") int maxResult) {
        this.serperClient = serperClient;
        this.allowlist = allowlist;
        this.maxResult = maxResult;
    }

    public static String buildAllowlistedQuery(String query, List<String> domains) {
        if(domains == null || domains.isEmpty()) return query;
        String sites = domains.stream().map(d -> "site:" + d).collect(Collectors.joining("OR"));
        return query + " " + sites;
    }

    public boolean isAllowed(String url) {
        try{
            String host = URI.create(url).getHost();
            if(host == null) return false;
            return allowlist.domains().stream().anyMatch(host::endsWith);
        } catch (Exception e) {
            return false;
        }
    }

    public Mono<List<LinkResult>> retrieve(String query, int m) {
        String searchQ = buildAllowlistedQuery(query, allowlist.domains());
        return serperClient.search(searchQ, maxResult).defaultIfEmpty(new SerperResponse(List.of())).map(response -> response.organic() ==null? List.<LinkResult>of(): response.organic().stream().filter(item -> isAllowed(item.link())).limit(maxResult).map(item -> new LinkResult(item.link(), item.title(), item.snippet())).collect(Collectors.toList()));
    }
}
