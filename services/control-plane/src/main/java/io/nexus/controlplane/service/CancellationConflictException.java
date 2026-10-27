package io.nexus.controlplane.service;

public class CancellationConflictException extends RuntimeException {
    public CancellationConflictException(String message) {
        super(message);
    }
}
