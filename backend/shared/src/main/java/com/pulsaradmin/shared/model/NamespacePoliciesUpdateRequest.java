package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record NamespacePoliciesUpdateRequest(
    @NotBlank String tenant,
    @NotBlank String namespace,
    NamespacePolicies policies,
    @NotBlank String reason) {
}
