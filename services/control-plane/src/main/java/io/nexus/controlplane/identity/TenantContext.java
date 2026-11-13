package io.nexus.controlplane.identity;

/** Supplied by a trusted server-side source, never from unverified request fields. */
public record TenantContext(String tenantId, String projectId) {
    public TenantContext {
        if (tenantId == null || projectId == null || tenantId.isBlank() || projectId.isBlank()
                || tenantId.length() > 100 || projectId.length() > 100) {
            throw new IllegalArgumentException("Tenant and project must be nonblank identifiers of at most 100 characters");
        }
    }
}
