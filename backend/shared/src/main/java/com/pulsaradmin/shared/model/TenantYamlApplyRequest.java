package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record TenantYamlApplyRequest(
    @NotBlank String previewId,
    @NotBlank String reason) {
}
