package com.pulsaradmin.shared.model;

public record PlatformArtifactMutationResponse(
    String environmentId,
    String artifactType,
    String action,
    String name,
    String message,
    PlatformArtifactDetails artifact,
    PlatformSummary platformSummary) {
}
