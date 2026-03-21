package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record NamespaceDeleteRequest(
    @NotBlank(message = "Tenant is required.")
    String tenant,
    @NotBlank(message = "Namespace is required.")
    String namespace,
    @NotBlank(message = "Reason is required.")
    String reason) {
}
