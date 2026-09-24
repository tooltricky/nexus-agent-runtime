package io.nexus.controlplane.api;

import io.nexus.controlplane.api.ApiModels.SessionResponse;
import io.nexus.controlplane.service.RuntimeService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/sessions")
public class SessionController {
    private final RuntimeService runtimeService;

    public SessionController(RuntimeService runtimeService) {
        this.runtimeService = runtimeService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public SessionResponse createSession() {
        return runtimeService.createSession();
    }
}

