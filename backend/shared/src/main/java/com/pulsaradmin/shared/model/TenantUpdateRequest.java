package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;
import java.util.List;

public record TenantUpdateRequest(
    @NotBlank(message = "Tenant is required.")
    String tenant,
    List<String> adminRoles,
    List<String> allowedClusters,
    @NotBlank(message = "Reason is required.")
    String reason) {
}
