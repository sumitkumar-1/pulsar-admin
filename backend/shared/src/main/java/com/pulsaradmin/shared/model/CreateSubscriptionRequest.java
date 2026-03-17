package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.NotBlank;

public record CreateSubscriptionRequest(
    @NotBlank(message = "Topic name is required.")
    String topicName,
    @NotBlank(message = "Subscription name is required.")
    String subscriptionName,
    @NotBlank(message = "Initial position is required.")
    String initialPosition,
    String reason) {
}
