package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record PlatformArtifactDeleteRequest(
    @NotBlank String artifactType,
    @NotBlank String name,
    String tenant,
    String namespace,
    @NotBlank String reason) {
}
