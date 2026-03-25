package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record ClearBacklogRequest(
    @NotBlank(message = "Topic name is required.")
    String topicName,
    @NotBlank(message = "Subscription name is required.")
    String subscriptionName,
    @NotBlank(message = "Reason is required.")
    String reason) {
}
