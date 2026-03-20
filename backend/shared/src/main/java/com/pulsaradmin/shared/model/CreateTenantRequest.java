package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;
import java.util.List;

public record CreateTenantRequest(
    @NotBlank String tenant,
    List<String> adminRoles,
    List<String> allowedClusters) {
}
