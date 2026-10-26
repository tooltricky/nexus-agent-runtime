package io.nexus.controlplane.api;

import io.nexus.controlplane.api.ApiModels.AcceptedRunResponse;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.api.ApiModels.RunResponse;
import io.nexus.controlplane.api.ApiModels.TimelineResponse;
import io.nexus.controlplane.service.RuntimeService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@RequestMapping("/v1/runs")
public class RunController {
    private final RuntimeService runtimeService;

    public RunController(RuntimeService runtimeService) {
        this.runtimeService = runtimeService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public AcceptedRunResponse createRun(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 255) String idempotencyKey,
            @Valid @RequestBody CreateRunRequest request
    ) {
        return runtimeService.createRun(idempotencyKey, request);
    }

    @GetMapping("/{runId}")
    public ResponseEntity<RunResponse> getRun(@PathVariable String runId) {
        return ResponseEntity.ok().header("Nexus-API-Contract", "0.2").body(runtimeService.getRun(runId));
    }

    @GetMapping("/{runId}/timeline")
    public ResponseEntity<TimelineResponse> getTimeline(@PathVariable String runId) {
        return ResponseEntity.ok().header("Nexus-API-Contract", "0.2").body(runtimeService.getTimeline(runId));
    }
}
