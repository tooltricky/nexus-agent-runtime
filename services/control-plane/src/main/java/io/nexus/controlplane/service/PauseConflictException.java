package io.nexus.controlplane.service;

public class PauseConflictException extends RuntimeException {
    public PauseConflictException(String message) { super(message); }
}
