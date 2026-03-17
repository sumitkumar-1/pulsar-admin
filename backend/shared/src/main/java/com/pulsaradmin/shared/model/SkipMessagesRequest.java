package com.pulsaradmin.shared.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record SkipMessagesRequest(
    @NotBlank(message = "Topic name is required.")
    String topicName,
    @NotBlank(message = "Subscription name is required.")
    String subscriptionName,
    @Min(value = 1, message = "Message count must be at least 1.")
    @Max(value = 5000, message = "Message count must be 5000 or less.")
    int messageCount,
    @NotBlank(message = "Reason is required.")
    String reason) {
}
