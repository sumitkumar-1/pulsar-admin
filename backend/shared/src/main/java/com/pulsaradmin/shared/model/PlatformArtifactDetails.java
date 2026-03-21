package com.pulsaradmin.shared.model;

public record PlatformArtifactDetails(
    String environmentId,
    String artifactType,
    String name,
    String tenant,
    String namespace,
    String status,
    String details,
    String archive,
    String className,
    String inputTopic,
    String outputTopic,
    Integer parallelism,
    String configs,
    boolean editable) {
}
