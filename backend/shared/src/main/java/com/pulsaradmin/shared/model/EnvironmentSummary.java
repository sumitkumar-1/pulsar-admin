package com.pulsaradmin.shared.model;

public record EnvironmentSummary(
    String id,
    String name,
    String kind,
    EnvironmentStatus status,
    String region,
    String clusterLabel,
    String summary) {
}
