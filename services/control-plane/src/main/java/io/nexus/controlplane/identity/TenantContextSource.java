package io.nexus.controlplane.identity;

/** Future verified identities plug in here. Implementations must fail closed. */
@FunctionalInterface
public interface TenantContextSource {
    TenantContext requireContext();
}
