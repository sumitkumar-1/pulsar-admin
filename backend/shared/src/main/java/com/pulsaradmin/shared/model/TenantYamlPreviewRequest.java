package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record TenantYamlPreviewRequest(
    @NotBlank String tenant,
    @NotBlank String namespace,
    @NotBlank String yaml) {
}
