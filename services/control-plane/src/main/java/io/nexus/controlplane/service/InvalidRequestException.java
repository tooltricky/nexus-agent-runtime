package io.nexus.controlplane.service;

public class InvalidRequestException extends RuntimeException {
    public InvalidRequestException(String message) { super(message); }
}
