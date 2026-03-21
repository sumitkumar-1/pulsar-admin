package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record TopicDeleteRequest(
    @NotBlank(message = "Topic name is required.")
    String topicName,
    @NotBlank(message = "Reason is required.")
    String reason) {
}
