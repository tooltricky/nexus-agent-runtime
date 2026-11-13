package io.nexus.controlplane.api;

import io.nexus.controlplane.api.ApiModels.ErrorResponse;
import io.nexus.controlplane.service.ConflictException;
import io.nexus.controlplane.service.CancellationConflictException;
import io.nexus.controlplane.service.PauseConflictException;
import io.nexus.controlplane.service.NotFoundException;
import jakarta.validation.ConstraintViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(io.nexus.controlplane.identity.IdentityUnavailableException.class)
    public ResponseEntity<ErrorResponse> identityUnavailable() {
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(new ErrorResponse("identity_unavailable", "Trusted tenant and project context is unavailable"));
    }

    @ExceptionHandler(PauseConflictException.class)
    public ResponseEntity<ErrorResponse> pauseConflict(PauseConflictException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorResponse("pause_conflict", exception.getMessage()));
    }

    @ExceptionHandler(CancellationConflictException.class)
    public ResponseEntity<ErrorResponse> cancellationConflict(CancellationConflictException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorResponse("cancellation_conflict", exception.getMessage()));
    }

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<ErrorResponse> notFound(NotFoundException exception) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse("not_found", exception.getMessage()));
    }

    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<ErrorResponse> conflict(ConflictException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorResponse("idempotency_conflict", exception.getMessage()));
    }

    @ExceptionHandler({
            io.nexus.controlplane.service.InvalidRequestException.class,
            MethodArgumentNotValidException.class,
            ConstraintViolationException.class,
            MissingRequestHeaderException.class,
            HttpMessageNotReadableException.class
    })
    public ResponseEntity<ErrorResponse> badRequest(Exception exception) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse("invalid_request", "Request validation failed"));
    }
}

