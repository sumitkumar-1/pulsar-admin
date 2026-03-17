package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record EnvironmentUpsertRequest(
    @NotBlank
    @Pattern(regexp = "[a-z0-9-]{2,32}", message = "Environment id must use lowercase letters, numbers, and dashes.")
    String id,
    @NotBlank
    @Size(max = 64)
    String name,
    @NotBlank
    @Size(max = 16)
    String kind,
    @NotBlank
    @Size(max = 64)
    String region,
    @NotBlank
    @Size(max = 64)
    String clusterLabel,
    @NotBlank
    @Size(max = 255)
    String summary,
    @NotBlank
    @Size(max = 255)
    String brokerUrl,
    @NotBlank
    @Size(max = 255)
    String adminUrl,
    @NotBlank
    @Size(max = 32)
    String authMode,
    @Size(max = 255)
    String credentialReference,
    boolean tlsEnabled) {
}
