package controller;

import jakarta.validation.Valid;
import model.RetrieverRequest;
import model.RetrieverResponse;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import service.RetrievalService;

import java.util.UUID;

@RestController

@RequestMapping ("/api/retriever")
public class RetrieverController {

    private final RetrievalService service;

    public RetrieverController(RetrievalService service) {
        this.service = service;
    }

    @PostMapping(value = "/search", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<RetrieverResponse> search(@Valid @RequestBody RetrieverRequest req) {
        String cid = (req.correlationId() != null && !req.correlationId().isBlank()?req.correlationId(): UUID.randomUUID().toString());
        return service.retrieve(req.query(), req.maxResults()).map(links -> new RetrieverResponse(cid, links));
    }
}
