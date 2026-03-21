package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;

public record PlatformArtifactMutationRequest(
    @NotBlank String artifactType,
    @NotBlank String name,
    String tenant,
    String namespace,
    String archive,
    String className,
    String inputTopic,
    String outputTopic,
    @Max(128) Integer parallelism,
    String configs,
    @NotBlank String reason) {
}
