package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record UnloadTopicRequest(
    @NotBlank String topicName,
    @NotBlank @Size(max = 240) String reason) {
}
