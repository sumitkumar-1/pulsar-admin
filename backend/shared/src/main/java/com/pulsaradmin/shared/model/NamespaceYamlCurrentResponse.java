package com.pulsaradmin.shared.model;

import java.time.Instant;

public record NamespaceYamlCurrentResponse(
    String environmentId,
    String tenant,
    String namespace,
    String yaml,
    String message,
    Instant generatedAt) {
}
