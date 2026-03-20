package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;
import java.util.Map;

public record PublishMessageRequest(
    @NotBlank String topicName,
    String key,
    Map<String, String> properties,
    String schemaMode,
    @NotBlank String payload,
    @NotBlank String reason) {
}
