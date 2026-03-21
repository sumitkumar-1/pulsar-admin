package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record SchemaDeleteRequest(
    @NotBlank String topicName,
    @NotBlank String reason) {
}
