package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record SchemaUpdateRequest(
    @NotBlank String topicName,
    @NotBlank String schemaType,
    String compatibility,
    @NotBlank String definition,
    @NotBlank String reason) {
}
