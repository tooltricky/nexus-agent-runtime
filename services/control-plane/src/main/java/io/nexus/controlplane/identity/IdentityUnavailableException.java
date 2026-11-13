package io.nexus.controlplane.identity;

public class IdentityUnavailableException extends RuntimeException {
    public IdentityUnavailableException() { super("Trusted tenant and project context is unavailable"); }
}
