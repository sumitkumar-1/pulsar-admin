package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record TenantDeleteRequest(
    @NotBlank(message = "Tenant is required.")
    String tenant,
    @NotBlank(message = "Reason is required.")
    String reason) {
}
