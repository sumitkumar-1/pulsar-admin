package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record TerminateTopicRequest(
    @NotBlank String topicName,
    @NotBlank String reason) {
}
