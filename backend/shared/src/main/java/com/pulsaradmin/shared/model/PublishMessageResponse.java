package com.pulsaradmin.shared.model;

import java.time.Instant;
import java.util.Map;

public record PublishMessageResponse(
    String environmentId,
    String topicName,
    String messageId,
    String key,
    Map<String, String> properties,
    String schemaMode,
    Instant publishedAt,
    String message) {
}
