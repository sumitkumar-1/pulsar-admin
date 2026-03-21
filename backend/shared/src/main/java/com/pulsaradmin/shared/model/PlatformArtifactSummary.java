package com.pulsaradmin.shared.model;

public record PlatformArtifactSummary(
    String name,
    String tenant,
    String namespace,
    String status,
    String details) {
}
