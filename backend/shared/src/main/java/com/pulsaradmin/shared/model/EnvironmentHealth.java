package com.pulsaradmin.shared.model;

public record EnvironmentHealth(
    String environmentId,
    EnvironmentStatus status,
    String brokerUrl,
    String adminUrl,
    String pulsarVersion,
    String message) {
}
